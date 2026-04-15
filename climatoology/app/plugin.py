import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import NoReturn
from uuid import UUID

import shapely
from celery import Celery
from kombu.entity import Exchange, Queue
from pydantic import BaseModel
from pydantic_extra_types.language_code import LanguageAlpha2
from shapely import MultiPolygon
from sqlalchemy.orm import Session

import climatoology
from climatoology.app.exception import VersionMismatchError
from climatoology.app.settings import EXCHANGE_NAME, CABaseSettings, WorkerSettings
from climatoology.app.tasks import CAPlatformComputeTask, run_standalone
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import (
    AoiFeatureModel,
    AoiProperties,
    StandAloneComputationInfo,
)
from climatoology.base.i18n import DEFAULT_LANGUAGE, deep_translate_dict, set_language, tr
from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import PluginInfoEnriched, PluginInfoFinal
from climatoology.base.utils import shapely_from_geojson_pydantic
from climatoology.store.database.database import BackendDatabase
from climatoology.store.database.models.plugin_info import PluginInfoTable
from climatoology.store.object_store import MinioStorage, Storage

log = get_climatoology_logger(__name__)


def start_plugin(operator: BaseOperator) -> NoReturn:
    """Start a CA Plugin

    :param operator: The Operator that fills the plugin with life
    :return: This method does not return until the plugin is stopped, then it will exit the application.
    """
    # we require the user to set the settings via a .env file or via env variables
    # noinspection PyArgumentList
    settings = CABaseSettings()
    plugin = _create_plugin(operator, settings)

    worker_config = WorkerSettings()
    worker_config_dict = worker_config.model_dump()
    worker_hostname = worker_config_dict.pop('worker_hostname')
    plugin.conf.update(**worker_config_dict)

    plugin.start(['worker', '-n', f'{plugin.main}@{worker_hostname}', '--loglevel', settings.log_level])


def _create_plugin(operator: BaseOperator, settings: CABaseSettings) -> Celery:
    plugin = Celery(
        operator.info_enriched.id,
        broker=settings.broker_connection_string,
        backend=settings.backend_connection_string,
    )
    plugin = configure_queue(settings, plugin)

    backend_database = BackendDatabase(
        connection_string=settings.db_connection_string,
        user_agent=f'Plugin {operator.info_enriched.id}/{operator.info_enriched.version} based on climatoology/{climatoology.__version__}',
    )

    assert _version_is_compatible(info=operator.info_enriched, db=backend_database, celery=plugin), (
        'The plugin version comparison failed.'
    )

    storage = MinioStorage(
        host=settings.minio_host,
        port=settings.minio_port,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        bucket=settings.minio_bucket,
        secure=settings.minio_secure,
    )

    _ = synch_info(info=operator.info_enriched, db=backend_database, storage=storage)

    compute_task = CAPlatformComputeTask(operator=operator, storage=storage, backend_db=backend_database)
    plugin.register_task(compute_task)

    return plugin


def configure_queue(settings: CABaseSettings, plugin: Celery) -> Celery:
    queue_name = plugin.main

    exchange = Exchange(name=EXCHANGE_NAME, type='direct')
    compute_queue = Queue(
        name=queue_name,
        exchange=exchange,
        routing_key=queue_name,
        queue_arguments={
            'x-dead-letter-exchange': settings.deadletter_exchange_name,
            'x-dead-letter-routing-key': settings.deadletter_channel_name,
        },
    )
    plugin.conf.task_queues = (compute_queue,)

    return plugin


def extract_plugin_id(plugin_id_with_suffix: str) -> str:
    return plugin_id_with_suffix.split('@')[0]


def _version_is_compatible(info: PluginInfoEnriched, db: BackendDatabase, celery: Celery) -> bool:
    with Session(db.engine) as session:
        info_query = session.query(PluginInfoTable).filter_by(id=info.id)
        existing_info = info_query.first()
    if existing_info:
        existing_info_version = existing_info.version
        incoming_info_version = info.version
        if existing_info_version > incoming_info_version:
            raise VersionMismatchError(
                f'Refusing to register plugin {info.name} in version {info.version}.'
                f'A newer version ({existing_info.version}) has previously been registered on the platform. If '
                f'this is intentional, manually downgrade your platform and be aware of or mitigate the '
                f'possible sideeffects!'
            )
        elif existing_info_version < incoming_info_version:
            workers = celery.control.inspect().ping() or dict()
            plugins = {extract_plugin_id(k) for k, _ in workers.items()}
            assert info.id not in plugins, (
                f'Refusing to register plugin {info.name} version {incoming_info_version} because a plugin with a lower version ({existing_info_version}) is already running. Make sure to stop it before upgrading.'
            )
            log.info(
                f'Accepting plugin upgrade for {info.name} from {existing_info_version} to {incoming_info_version}'
            )
        else:
            log.debug(
                f'Registering {info.name} version {incoming_info_version} which is the same as the previously registered version ({existing_info_version})'
            )
    return True


def synch_info(
    info: PluginInfoEnriched, db: BackendDatabase, storage: Storage
) -> dict[LanguageAlpha2, PluginInfoFinal]:
    final_assets = storage.write_assets(plugin_id=info.id, assets=info.assets)

    synced_info = dict()
    for lang in info.purpose.keys():
        set_language(lang=lang, localisation_dir=info.assets.localisation_directory)

        unchanged_info = info.model_dump(exclude={'teaser', 'purpose', 'methodology', 'assets', 'operator_schema'})
        translated_teaser = tr(info.teaser)
        translated_schema = translate_operator_schema(schema=info.operator_schema)

        final_info = PluginInfoFinal(
            **unchanged_info,
            teaser=translated_teaser,
            purpose=info.purpose[lang],
            methodology=info.methodology[lang],
            language=lang,
            assets=final_assets,
            operator_schema=translated_schema,
        )
        _ = db.write_info(info=final_info)
        synced_info[lang] = final_info

    return synced_info


def translate_operator_schema(schema: dict) -> dict:
    translated_schema = deep_translate_dict(data=schema, target_keys={'title', 'description'})

    definitions = translated_schema.get('$defs', {})
    for name, definition in definitions.items():
        enum_values = definition.get('enum')
        if enum_values:
            enum_translation = {}
            for v in enum_values:
                enum_translation[v] = tr(v)
            definition['x-translation'] = enum_translation

    return translated_schema


def run_standalone_computation(
    operator: BaseOperator,
    params: BaseModel,
    aoi_geom: shapely.MultiPolygon = None,
    aoi_properties: AoiProperties = None,
    aoi_file: Path = None,
    lang: str = DEFAULT_LANGUAGE,
    output_dir: Path = None,
    computation_id: UUID = None,
) -> StandAloneComputationInfo:
    """
    Run a stand-alone computation without connecting to the CA platform.

    :param operator: The Operator that fills the plugin with life
    :param params: The plugin input parameters
    :param aoi_geom: The target area of interest. Required with aoi_properties. These take precedence over aoi_file.
    :param aoi_properties: Properties of the target area. Required with aoi_geom. These take precedence over aoi_file
    :param aoi_file: A geojson file containing a single geojson Feature of the aoi (see aoi_geom) with its properties
    (see aoi_properties). If not specified, aoi_geom and aoi_properties are required.
    :param lang: The language the results should be created in. Defaults to 'en'
    :param output_dir: The directory the results should be placed in. Defaults to
    results/<now-timestamp>-<aoi-name>-<compuation-id>.
    :param computation_id: The id of the computation to run. Will be reflected in the metadata. Defaults to a random
    UUID.
    :return: a StandAloneComputationInfo object containing all metadata of the completed computation
    """
    log.info('Starting stand-alone computation')
    if computation_id is None:
        computation_id = uuid.uuid4()

    if aoi_geom is None or aoi_properties is None:
        if aoi_file is None:
            raise ValueError('Either aoi_file or (aoi_geom and aoi_properties) must be provided')
        log.debug(f'aoi_geom and aoi_properties not given, reading aoi_file from {aoi_file.absolute()}')
        aoi = json.loads(aoi_file.read_text())
        aoi_feature = AoiFeatureModel(**aoi)
        # through difficult typing above we know it's a MultiPolygon but the type checker cannot know
        # noinspection PyTypeChecker
        aoi_geom: MultiPolygon = shapely_from_geojson_pydantic(geojson_geom=aoi_feature.geometry)
        aoi_properties = aoi_feature.properties

    if output_dir is None:
        output_dir = Path(f'results/{datetime.now()}-{aoi_properties.name}-{computation_id}')
    output_dir.mkdir(parents=True)

    computation_info = run_standalone(
        operator=operator,
        computation_id=computation_id,
        aoi_geom=aoi_geom,
        aoi_properties=aoi_properties,
        lang=lang,
        output_dir=output_dir,
        params=params,
    )
    log.info('Finished stand-alone computation')
    return computation_info
