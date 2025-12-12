import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Generator, List, Set
from unittest.mock import Mock, patch

import pytest
import responses
import shapely
import sqlalchemy
from celery import Celery, signals
from celery.backends.database import TaskExtended
from celery.backends.database.session import ResultModelBase
from celery.utils.threads import LocalStack
from freezegun import freeze_time
from pydantic import BaseModel, Field, HttpUrl
from pytest_alembic import Config
from pytest_postgresql.janitor import DatabaseJanitor
from semver import Version
from shapely import Polygon, set_srid
from sqlalchemy import String, cast, create_engine, insert, text, update
from sqlalchemy.orm import Session

from climatoology.app.plugin import _create_plugin
from climatoology.app.settings import CABaseSettings
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.artifact import (
    ARTIFACT_OVERWRITE_FIELDS,
    Artifact,
    ArtifactEnriched,
    ArtifactMetadata,
    ArtifactModality,
)
from climatoology.base.artifact_creators import create_markdown_artifact
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import (
    AoiFeatureModel,
    AoiProperties,
    ComputationInfo,
    ComputationPluginInfo,
    ComputationResources,
    ComputationScope,
    ComputationState,
)
from climatoology.base.plugin_info import (
    AssetsFinal,
    Concern,
    MiscSource,
    PluginAuthor,
    PluginInfo,
    PluginInfoEnriched,
    PluginInfoFinal,
)
from climatoology.store.database.database import BackendDatabase
from climatoology.store.database.models.base import ClimatoologyTableBase
from climatoology.store.database.models.computation import ComputationTable
from climatoology.store.database.models.views import (
    ArtifactErrorsView,
    ComputationsSummaryView,
    FailedComputationsView,
    UsageView,
    ValidComputationsView,
    create_view_tracking_object,
)
from climatoology.store.object_store import MinioStorage
from climatoology.utility.api import HealthCheck

pytest_plugins = ('celery.contrib.pytest',)


@pytest.fixture
def set_basic_envs(monkeypatch):
    monkeypatch.setenv('minio_host', 'test.host')
    monkeypatch.setenv('minio_port', '1234')
    monkeypatch.setenv('minio_access_key', 'minio_test_key')
    monkeypatch.setenv('minio_secret_key', 'minio_test_secret')
    monkeypatch.setenv('minio_bucket', 'minio_test_bucket')

    monkeypatch.setenv('rabbitmq_host', 'test.host')
    monkeypatch.setenv('rabbitmq_port', '1234')
    monkeypatch.setenv('rabbitmq_user', 'test_user')
    monkeypatch.setenv('rabbitmq_password', 'test_pw')

    monkeypatch.setenv('postgres_host', 'test.host')
    monkeypatch.setenv('postgres_port', '1234')
    monkeypatch.setenv('postgres_database', 'test_database')
    monkeypatch.setenv('postgres_user', 'test_user')
    monkeypatch.setenv('postgres_password', 'test_password')


@pytest.fixture
def default_settings(set_basic_envs) -> CABaseSettings:
    # the base settings are read from the env vars that are provided to this fixture
    # noinspection PyArgumentList
    return CABaseSettings()


@pytest.fixture
def general_uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def frozen_time():
    with freeze_time(datetime(2018, 1, 1, 12, tzinfo=UTC), ignore=['celery']) as frozen_time:
        yield frozen_time


@pytest.fixture
def default_plugin_key() -> str:
    return 'test_plugin;3.1.0'


@pytest.fixture
def default_plugin_info(default_input_model) -> PluginInfo:
    info = PluginInfo(
        name='Test Plugin',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        icon=Path(__file__).parent / 'resources/test_icon.png',
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent / 'resources/test_methodology.md',
        sources_library=Path(__file__).parent / 'resources/test.bib',
        computation_shelf_life=timedelta(days=1),
        demo_input_parameters=default_input_model,
    )
    info.version = Version(3, 1, 0)
    return info


@pytest.fixture
def default_plugin_info_enriched(default_operator) -> PluginInfoEnriched:
    return default_operator.info_enriched


@pytest.fixture
def default_plugin_info_final(default_plugin_info_enriched) -> PluginInfoFinal:
    assets = AssetsFinal(icon='assets/test_plugin/latest/ICON.png')
    default_info_final = PluginInfoFinal(**default_plugin_info_enriched.model_dump(exclude={'assets'}), assets=assets)
    return default_info_final


@pytest.fixture
def default_artifact_metadata() -> ArtifactMetadata:
    """Note: this should only provide required fields (except filename, which would make testing very cumbersome).
    This way it automatically is a test that optional fields are in fact optional.
    """
    return ArtifactMetadata(name='test_name', filename='test_artifact_file', summary='Test summary')


@pytest.fixture
def extensive_artifact_metadata(default_association_tags) -> ArtifactMetadata:
    return ArtifactMetadata(
        name='test_name',
        primary=False,
        tags=default_association_tags,
        filename='test_artifact_file',
        summary='Test summary',
        description='Test description',
        sources={'key1', 'key2'},
    )


@pytest.fixture
def default_artifact(general_uuid, default_artifact_metadata) -> Artifact:
    """Note: this should only provide required fields (except filename, which would make testing very cumbersome).
    This way it automatically is a test that optional fields are in fact optional.
    """
    return Artifact(
        **default_artifact_metadata.model_dump(exclude={'filename'}),
        modality=ArtifactModality.MARKDOWN,
        filename='test_artifact_file.md',
    )


@pytest.fixture
def extensive_artifact(general_uuid, extensive_artifact_metadata) -> Artifact:
    """Note: this should alter ALL fields (including optional ones,
    except rank and attachments, which would make testing very cumbersome).
    """
    return Artifact(
        **extensive_artifact_metadata.model_dump(exclude=ARTIFACT_OVERWRITE_FIELDS),
        modality=ArtifactModality.MARKDOWN,
        filename='test_artifact_file.md',
    )


@pytest.fixture
def default_artifact_enriched(default_artifact, general_uuid) -> ArtifactEnriched:
    return ArtifactEnriched(**default_artifact.model_dump(exclude={'sources'}), rank=0, correlation_uuid=general_uuid)


@pytest.fixture
def extensive_artifact_enriched(extensive_artifact, general_uuid) -> ArtifactEnriched:
    return ArtifactEnriched(
        **extensive_artifact.model_dump(exclude={'sources'}),
        rank=0,
        correlation_uuid=general_uuid,
        sources=[MiscSource(ID='id', title='title', author='author', year='2025', ENTRYTYPE='misc', url='https://a.b')],
    )


class TestModel(BaseModel):
    __test__ = False
    id: int = Field(title='ID', description='A required integer parameter.', examples=[1])
    name: str = Field(
        title='Name', description='An optional name parameter.', examples=['John Doe'], default='John Doe'
    )
    execution_time: float = Field(
        title='Execution time',
        description='The time for the compute to run (in seconds)',
        examples=[10.0],
        default=0.0,
    )


@pytest.fixture
def default_input_model() -> TestModel:
    return TestModel(id=1)


@pytest.fixture
def default_operator(default_plugin_info, default_artifact_metadata) -> BaseOperator:
    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> PluginInfo:
            return default_plugin_info.model_copy(deep=True)

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[Artifact]:
            time.sleep(params.execution_time)
            artifact_text = (Path(__file__).parent / 'resources/test_artifact_file.md').read_text()
            artifact = create_markdown_artifact(
                text=artifact_text,
                metadata=default_artifact_metadata,
                resources=resources,
            )
            return [artifact]

    return TestOperator()


@pytest.fixture
def default_plugin(
    celery_app, celery_worker, default_operator, default_settings, mocked_object_store, default_backend_db
) -> Generator[Celery, None, None]:
    with (
        patch('climatoology.app.plugin.Celery', return_value=celery_app),
        patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db),
    ):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        celery_worker.reload()
        yield plugin


@pytest.fixture
def default_aoi_geom_shapely() -> shapely.MultiPolygon:
    geom = shapely.MultiPolygon(polygons=[Polygon(((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0)))])
    srid_geom = set_srid(geometry=geom, srid=4326)
    return srid_geom


@pytest.fixture
def default_aoi_feature_geojson_pydantic(
    default_aoi_properties,
) -> AoiFeatureModel:
    return AoiFeatureModel(
        **{
            'type': 'Feature',
            'properties': default_aoi_properties.model_dump(mode='json'),
            'geometry': {
                'type': 'MultiPolygon',
                'coordinates': [
                    [
                        [
                            [0.0, 0.0],
                            [0.0, 1.0],
                            [1.0, 1.0],
                            [0.0, 0.0],
                        ]
                    ]
                ],
            },
        }
    )


@pytest.fixture
def default_aoi_feature_pure_dict(default_aoi_feature_geojson_pydantic) -> dict:
    return default_aoi_feature_geojson_pydantic.model_dump(mode='json')


@pytest.fixture
def default_aoi_properties() -> AoiProperties:
    return AoiProperties(name='test_aoi', id='test_aoi_id')


@pytest.fixture
def default_computation_resources(general_uuid) -> Generator[ComputationResources, None, None]:
    with ComputationScope(general_uuid) as resources:
        yield resources


@pytest.fixture
def mocked_utility_response():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost/health', json=HealthCheck().model_dump())
        yield rsps


@pytest.fixture
def mocked_object_store(minio_mock, default_settings) -> MinioStorage:
    minio_storage = MinioStorage(
        host=default_settings.minio_host,
        port=default_settings.minio_port,
        access_key=default_settings.minio_access_key,
        secret_key=default_settings.minio_secret_key,
        secure=True,
        bucket=default_settings.minio_bucket,
    )
    return minio_storage


@pytest.fixture
def default_computation_info(
    general_uuid, default_aoi_feature_geojson_pydantic, default_artifact_enriched, default_plugin_info
) -> ComputationInfo:
    return ComputationInfo(
        correlation_uuid=general_uuid,
        request_ts=datetime(2018, 1, 1, 12),
        deduplication_key=uuid.UUID('24209215-3397-e96c-2bf2-084116c66532'),
        cache_epoch=17532,
        valid_until=datetime(2018, 1, 2),
        params={'id': 1, 'name': 'John Doe', 'execution_time': 0.0},
        requested_params={'id': 1},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[default_artifact_enriched],
        plugin_info=ComputationPluginInfo(id=default_plugin_info.id, version=default_plugin_info.version),
    )


@pytest.fixture
def default_computation_task(
    default_operator, mocked_object_store, default_backend_db, general_uuid
) -> CAPlatformComputeTask:
    compute_task = CAPlatformComputeTask(
        operator=default_operator, storage=mocked_object_store, backend_db=default_backend_db
    )
    compute_task.update_state = Mock()
    request = Mock()
    request.correlation_id = str(general_uuid)
    compute_task.request_stack = LocalStack()
    compute_task.request_stack.push(request)
    return compute_task


@pytest.fixture
def celery_worker_parameters():
    return {'hostname': 'test_plugin@hostname'}


@signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    """
    Override the default celery logging setup, which would otherwise override the root logger. By overriding the root
    logger, the pytest `caplog` fixture no longer works, because the `caplog` handler gets removed. For more info, see
    the warning here: https://docs.pytest.org/en/latest/how-to/logging.html#caplog-fixture

    Note that celery has a setting `worker_hijack_root_logger`, but this is somewhat misleading. Even if you set this to
    `False`, celery still interferes with the root logger.  This is intentional
    behaviour: https://github.com/celery/celery/pull/2016
    """
    pass


@pytest.fixture
def default_association_tags() -> Set[StrEnum]:
    class ArtifactAssociation(StrEnum):
        TAG_A = 'Tag A'
        TAG_B = 'Tag B'

    return {ArtifactAssociation.TAG_A, ArtifactAssociation.TAG_B}


@pytest.fixture
def db_connection_params(request) -> dict:
    if os.getenv('CI', 'False').lower() == 'true':
        return {
            'host': os.getenv('POSTGRES_HOST'),
            'port': os.getenv('POSTGRES_PORT'),
            'database': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASSWORD'),
        }
    else:
        postgresql = request.getfixturevalue('postgresql')
        return {
            'host': postgresql.info.host,
            'port': postgresql.info.port,
            'database': postgresql.info.dbname,
            'user': postgresql.info.user,
            'password': postgresql.info.password,
        }


@pytest.fixture
def db_connection_string(db_connection_params) -> str:
    host = db_connection_params['host']
    port = db_connection_params['port']
    dbname = db_connection_params['database']
    user = db_connection_params['user']
    password = db_connection_params['password']

    if os.getenv('CI', 'False').lower() == 'true':
        db_janitor = DatabaseJanitor(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            version=int(os.getenv('POSTGRES_VERSION')),
        )
        db_janitor.drop()
        db_janitor.init()

    return f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}'


@pytest.fixture
def db_with_postgis(db_connection_string) -> str:
    with create_engine(db_connection_string).connect() as con:
        con.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        con.commit()
    return db_connection_string


@pytest.fixture
def db_with_tables(db_with_postgis, alembic_runner) -> str:
    engine = create_engine(db_with_postgis)
    with engine.connect() as con:
        con.execute(text('CREATE SCHEMA ca_base;'))
        con.commit()

    ResultModelBase.metadata.create_all(engine)
    ClimatoologyTableBase.metadata.create_all(engine)

    with engine.connect() as con:
        con.execute(create_view_tracking_object(ValidComputationsView).to_sql_statement_create())
        con.execute(create_view_tracking_object(ComputationsSummaryView).to_sql_statement_create())
        con.execute(create_view_tracking_object(UsageView).to_sql_statement_create())
        con.execute(create_view_tracking_object(FailedComputationsView).to_sql_statement_create())
        con.execute(create_view_tracking_object(ArtifactErrorsView).to_sql_statement_create())
        con.commit()

    alembic_runner.raw_command('stamp', 'head')
    return db_with_postgis


@pytest.fixture
def default_backend_db(db_with_tables) -> BackendDatabase:
    return BackendDatabase(
        connection_string=db_with_tables, user_agent='Test Climatoology Backend', assert_db_status=True
    )


@pytest.fixture
def backend_with_computation_registered(
    default_backend_db,
    default_computation_info,
    default_plugin_info_final,
    default_plugin_key,
    set_basic_envs,
    frozen_time,
) -> BackendDatabase:
    default_backend_db.write_info(info=default_plugin_info_final)
    default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_plugin_info_final.computation_shelf_life,
    )
    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info.correlation_uuid, params=default_computation_info.params
    )
    with Session(default_backend_db.engine) as session:
        session.execute(
            insert(TaskExtended).values(
                id='1', task_id=default_computation_info.correlation_uuid, status=ComputationState.PENDING
            )
        )
        session.commit()
    return default_backend_db


@pytest.fixture
def backend_with_computation_successful(backend_with_computation_registered, default_computation_info):
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(
            update(TaskExtended)
            .values(status=ComputationState.SUCCESS, date_done=datetime.now())
            .where(TaskExtended.task_id == cast(default_computation_info.correlation_uuid, String))
        )
        session.execute(update(ComputationTable).values(valid_until=datetime.now() + timedelta(hours=12)))
        session.commit()
    backend_with_computation_registered.update_successful_computation(computation_info=default_computation_info)
    return backend_with_computation_registered


@pytest.fixture
def alembic_config(
    general_uuid, default_plugin_info_final, default_computation_info, default_artifact_enriched
) -> Config:
    return Config(
        config_options={'script_location': 'climatoology/store/database/migration'},
        at_revision_data={
            '0c77b1f5b970': [
                {
                    '__tablename__': 'celery_taskmeta',
                    'id': 1,
                    'task_id': str(general_uuid),
                    'date_done': datetime.now(),
                }
            ],
            '3d4313578291': [
                {
                    '__tablename__': 'info',
                    'plugin_id': default_plugin_info_final.id,
                    'name': default_plugin_info_final.name,
                    'version': str(default_plugin_info_final.version),
                    'concerns': ['CLIMATE_ACTION__GHG_EMISSION'],
                    'purpose': default_plugin_info_final.purpose,
                    'methodology': default_plugin_info_final.methodology,
                    'assets': default_plugin_info_final.assets.model_dump(mode='json'),
                    'operator_schema': default_plugin_info_final.operator_schema,
                    'library_version': str(default_plugin_info_final.library_version),
                },
                {'__tablename__': 'pluginauthor', 'name': 'Waldemar'},
                {
                    '__tablename__': 'author_info_link_table',
                    'info_id': default_plugin_info_final.id,
                    'author_id': 'Waldemar',
                },
            ],
            '49cccfd144a8': [
                {
                    '__tablename__': 'ca-base.computation',
                    'correlation_uuid': str(general_uuid),
                    'timestamp': datetime.now(),
                    'params': default_computation_info.params,
                    'aoi_geom': default_computation_info.aoi.geometry.wkt,
                    'aoi_name': default_computation_info.aoi.properties.name,
                    'aoi_id': default_computation_info.aoi.properties.id,
                    'plugin_id': default_computation_info.plugin_info.id,
                    'plugin_version': str(default_computation_info.plugin_info.version),
                    'status': ComputationState.PENDING,
                    'artifact_errors': default_computation_info.artifact_errors,
                },
                {
                    '__tablename__': 'ca-base.artifact',
                    'correlation_uuid': str(general_uuid),
                    'name': default_artifact_enriched.name,
                    'modality': default_artifact_enriched.modality.value,
                    'primary': default_artifact_enriched.primary,
                    'summary': default_artifact_enriched.summary,
                    'store_id': default_artifact_enriched.filename,
                    'file_path': default_artifact_enriched.filename,
                },
            ],
            '8b52ceba3457': [
                {
                    '__tablename__': 'ca-base.computation_lookup',
                    'user_correlation_uuid': str(general_uuid),
                    'request_ts': datetime.now(),
                    'computation_id': str(general_uuid),
                }
            ],
        },
    )


@pytest.fixture
def alembic_engine(db_with_postgis, set_basic_envs):
    return sqlalchemy.create_engine(db_with_postgis)


@pytest.fixture
def default_sources() -> list[MiscSource]:
    return [
        MiscSource(
            ID='CitekeyMisc',
            title="Pluto: The 'Other' Red Planet",
            author='{NASA}',
            year='2015',
            note='Accessed: 2018-12-06',
            ENTRYTYPE='misc',
            url='https://www.nasa.gov/nh/pluto-the-other-red-planet',
        )
    ]
