from typing import Optional

from celery import Celery
from kombu.entity import Exchange, Queue
from sqlalchemy.orm import Session

import climatoology
from climatoology.app.exception import VersionMismatchError
from climatoology.app.settings import EXCHANGE_NAME, CABaseSettings, WorkerSettings
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.info import _Info
from climatoology.base.logging import get_climatoology_logger
from climatoology.store.database.database import BackendDatabase
from climatoology.store.database.models.info import InfoTable
from climatoology.store.object_store import MinioStorage, Storage

log = get_climatoology_logger(__name__)


def start_plugin(operator: BaseOperator) -> Optional[int]:
    """Start a CA Plugin

    :param operator: The Operator that fills the plugin with life
    :return: This method does not return until the plugin is stopped. Then it will return the exit code
    """
    # we require the user to set the settings via a .env file or via env variables
    # noinspection PyArgumentList
    settings = CABaseSettings()
    plugin = _create_plugin(operator, settings)

    worker_config = WorkerSettings()
    worker_config_dict = worker_config.model_dump()
    worker_hostname = worker_config_dict.pop('worker_hostname')
    plugin.conf.update(**worker_config_dict)

    return plugin.start(['worker', '-n', f'{plugin.main}@{worker_hostname}', '--loglevel', settings.log_level])


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


def _version_is_compatible(info: _Info, db: BackendDatabase, celery: Celery) -> bool:
    with Session(db.engine) as session:
        info_query = session.query(InfoTable).filter_by(id=info.id)
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


def synch_info(info: _Info, db: BackendDatabase, storage: Storage) -> _Info:
    info.assets = storage.write_assets(plugin_id=info.id, assets=info.assets)

    _ = db.write_info(info=info)

    return info
