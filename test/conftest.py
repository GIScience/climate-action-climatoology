import os
import time
import uuid
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Generator, List, Set
from unittest.mock import Mock, patch

import geojson_pydantic
import pytest
import responses
import shapely
from celery import Celery
from celery.utils.threads import LocalStack
from kombu import Exchange, Queue
from pydantic import BaseModel, Field, HttpUrl
from pytest_postgresql.janitor import DatabaseJanitor
from semver import Version
from shapely import set_srid
from sqlalchemy import create_engine, text

import climatoology
from climatoology.app.platform import CeleryPlatform
from climatoology.app.plugin import _create_plugin
from climatoology.app.settings import EXCHANGE_NAME, CABaseSettings
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.info import Concern, PluginAuthor, _Info, generate_plugin_info
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import ComputationInfo, MinioStorage, PluginBaseInfo
from climatoology.utility.api import HealthCheck

pytest_plugins = ('celery.contrib.pytest',)


@pytest.fixture
def set_basic_envs(monkeypatch):
    monkeypatch.setenv('minio_host', 'minio.test')
    monkeypatch.setenv('minio_port', '1000')
    monkeypatch.setenv('minio_access_key', 'minio_test_key')
    monkeypatch.setenv('minio_secret_key', 'minio_test_secret')
    monkeypatch.setenv('minio_bucket', 'minio_test_bucket')

    monkeypatch.setenv('rabbitmq_host', 'test-host')
    monkeypatch.setenv('rabbitmq_port', '1234')
    monkeypatch.setenv('rabbitmq_user', 'test_user')
    monkeypatch.setenv('rabbitmq_password', 'test_pw')

    monkeypatch.setenv('postgres_host', 'test-host')
    monkeypatch.setenv('postgres_port', '1234')
    monkeypatch.setenv('postgres_user', 'test_user')
    monkeypatch.setenv('postgres_password', 'test_password')
    monkeypatch.setenv('postgres_database', 'test_database')


@pytest.fixture
def default_settings(set_basic_envs) -> CABaseSettings:
    # the base settings are read from the env vars that are provided to this fixture
    # noinspection PyArgumentList
    return CABaseSettings()


@pytest.fixture
def general_uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def stop_time(time_machine):
    time_machine.move_to(datetime(2018, 1, 1, 12, tzinfo=UTC), tick=False)


@pytest.fixture
def default_info() -> _Info:
    info = generate_plugin_info(
        name='Test Plugin',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        icon=Path(__file__).parent / 'resources/test_icon.jpeg',
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent / 'resources/test.bib',
        demo_input_parameters=TestModel(id=1),
        demo_aoi=Path(__file__).parent / 'resources/test_aoi.geojson',
        computation_shelf_life=timedelta(days=1),
    )
    return info


@pytest.fixture
def default_info_enriched(default_info) -> _Info:
    default_info_enriched = default_info.model_copy(deep=True)
    default_info_enriched.library_version = str(climatoology.__version__)
    default_info_enriched.operator_schema = {
        'properties': {
            'id': {'description': 'A required integer parameter.', 'examples': [1], 'title': 'ID', 'type': 'integer'},
            'name': {
                'default': 'John Doe',
                'description': 'An optional name parameter.',
                'examples': ['John Doe'],
                'title': 'Name',
                'type': 'string',
            },
            'execution_time': {
                'default': 0.0,
                'description': 'The time for the compute to run (in seconds)',
                'examples': [10.0],
                'title': 'Execution time',
                'type': 'number',
            },
        },
        'required': ['id'],
        'title': 'TestModel',
        'type': 'object',
    }
    return default_info_enriched


@pytest.fixture
def default_info_final(default_info_enriched) -> _Info:
    default_info_final = default_info_enriched.model_copy(deep=True)
    default_info_final.assets.icon = 'assets/test_plugin/latest/ICON.jpeg'
    return default_info_final


@pytest.fixture
def default_artifact(general_uuid) -> _Artifact:
    return _Artifact(
        name='test_name',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path(__file__).parent / 'resources/test_artifact_file.md',
        summary='Test summary',
        description='Test description',
        correlation_uuid=general_uuid,
        store_id=f'{general_uuid}_test_artifact_file.md',
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
def default_operator(default_info, default_artifact) -> BaseOperator:
    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy(deep=True)

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[_Artifact]:
            time.sleep(params.execution_time)
            return [default_artifact]

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
    geom = shapely.MultiPolygon(polygons=[[((0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (0.0, 0.0))]])
    srid_geom = set_srid(geometry=geom, srid=4326)
    return srid_geom


@pytest.fixture
def default_aoi_feature_geojson_pydantic() -> geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties]:
    return geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
        **{
            'type': 'Feature',
            'properties': {'name': 'test_aoi', 'id': 'test_aoi_id'},
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
def default_computation_resources(general_uuid) -> Generator[ComputationScope, None, None]:
    with ComputationScope(general_uuid) as resources:
        yield resources


@pytest.fixture
def mocked_utility_response():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost:80/health', json=HealthCheck().model_dump())
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
    general_uuid, default_aoi_feature_geojson_pydantic, default_artifact, default_info
) -> ComputationInfo:
    return ComputationInfo(
        correlation_uuid=general_uuid,
        timestamp=datetime(2018, 1, 1, 12),
        deduplication_key=uuid.UUID('397e25df-3445-42a1-7e49-03466b3be5ca'),
        cache_epoch=17532,
        valid_until=datetime(2018, 1, 2),
        params={'id': 1, 'name': 'John Doe', 'execution_time': 0.0},
        requested_params={'id': 1},
        aoi=default_aoi_feature_geojson_pydantic,
        artifacts=[default_artifact],
        plugin_info=PluginBaseInfo(plugin_id=default_info.plugin_id, plugin_version=default_info.version),
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
    request.correlation_id = general_uuid
    compute_task.request_stack = LocalStack()
    compute_task.request_stack.push(request)
    return compute_task


@pytest.fixture
def default_platform_connection(
    celery_app, mocked_object_store, set_basic_envs, default_backend_db
) -> Generator[CeleryPlatform, None, None]:
    with (
        patch('climatoology.app.platform.CeleryPlatform.construct_celery_app', return_value=celery_app),
        patch('climatoology.app.platform.CeleryPlatform.construct_storage', return_value=mocked_object_store),
        patch('climatoology.app.platform.BackendDatabase', return_value=default_backend_db),
    ):
        yield CeleryPlatform()


@pytest.fixture
def celery_worker_parameters():
    return {'hostname': 'test_plugin@hostname'}


@pytest.fixture
def celery_app(celery_app):
    # Configure queues in the 'parent' celery_app because we aren't running rabbitmq for real
    compute_queue = Queue('test_plugin', Exchange(EXCHANGE_NAME), 'test_plugin')
    celery_app.amqp.queues.select_add(compute_queue)

    yield celery_app


@pytest.fixture
def default_association_tags() -> Set[StrEnum]:
    class ArtifactAssociation(StrEnum):
        TAG_A = 'Tag A'
        TAG_B = 'Tag B'

    return {ArtifactAssociation.TAG_A, ArtifactAssociation.TAG_B}


@pytest.fixture
def default_backend_db(request) -> BackendDatabase:
    if os.getenv('CI', 'False').lower() == 'true':
        pg_host = os.getenv('POSTGRES_HOST')
        pg_port = os.getenv('POSTGRES_PORT')
        pg_user = os.getenv('POSTGRES_USER')
        pg_password = os.getenv('POSTGRES_PASSWORD')
        pg_db = os.getenv('POSTGRES_DB')
        pg_version = int(os.getenv('POSTGRES_VERSION'))

        db_janitor = DatabaseJanitor(
            host=pg_host,
            port=pg_port,
            user=pg_user,
            password=pg_password,
            dbname=pg_db,
            version=Version(pg_version),
        )
        db_janitor.drop()
        db_janitor.init()

        connection_string = f'postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}'
    else:
        postgresql = request.getfixturevalue('postgresql')
        connection_string = f'postgresql+psycopg2://{postgresql.info.user}:{postgresql.info.password}@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'

    with create_engine(connection_string).connect() as con:
        con.execute(text('CREATE EXTENSION IF NOT EXISTS postgis;'))
        con.commit()
    return BackendDatabase(connection_string=connection_string, user_agent='Test Climatoology Backend')


@pytest.fixture
def backend_with_computation(
    default_backend_db, default_computation_info, default_info_final, set_basic_envs, stop_time
) -> BackendDatabase:
    default_backend_db.write_info(info=default_info_final)
    default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_id=default_computation_info.plugin_info.plugin_id,
        plugin_version=default_computation_info.plugin_info.plugin_version,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )
    return default_backend_db
