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
import sqlalchemy
from celery import Celery
from celery.utils.threads import LocalStack
from freezegun import freeze_time
from pydantic import BaseModel, Field, HttpUrl
from pytest_alembic import Config
from pytest_postgresql.janitor import DatabaseJanitor
from semver import Version
from shapely import set_srid
from sqlalchemy import String, cast, create_engine, insert, text, update
from sqlalchemy.orm import Session

import climatoology
from climatoology.app.plugin import _create_plugin
from climatoology.app.settings import CABaseSettings
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.event import ComputationState
from climatoology.base.info import (
    Concern,
    MiscSource,
    PluginAuthor,
    PluginBaseInfo,
    _Info,
    compose_demo_config,
    generate_plugin_info,
)
from climatoology.store.database.database import BackendDatabase
from climatoology.store.database.models.celery import CeleryTaskMeta
from climatoology.store.database.models.computation import ComputationTable
from climatoology.store.object_store import ComputationInfo, MinioStorage
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
        icon=Path(__file__).parent / 'resources/test_icon.png',
        version=Version(3, 1, 0),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent / 'resources/test.bib',
        computation_shelf_life=timedelta(days=1),
        demo_config=compose_demo_config(input_parameters=TestModel(id=1, name='John Doe', execution_time=0.0)),
    )
    return info


@pytest.fixture
def default_info_enriched(default_info) -> _Info:
    default_info_enriched = default_info.model_copy(deep=True)
    default_info_enriched.library_version = climatoology.__version__
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
    default_info_final.assets.icon = 'assets/test_plugin/latest/ICON.png'
    return default_info_final


@pytest.fixture
def default_artifact() -> _Artifact:
    """Note: this should only provide required fields.
    This way it automatically is a test that optional fields are in fact optional.
    """
    return _Artifact(
        name='test_name',
        modality=ArtifactModality.MARKDOWN,
        file_path=Path(__file__).parent / 'resources/test_artifact_file.md',
        summary='Test summary',
    )


@pytest.fixture
def default_augmented_artifact(default_artifact, general_uuid) -> _Artifact:
    final_artifact = default_artifact.model_copy(deep=True)
    final_artifact.correlation_uuid = general_uuid
    final_artifact.store_id = f'{general_uuid}_test_artifact_file.md'
    return final_artifact


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
def default_aoi_feature_geojson_pydantic(
    default_aoi_properties,
) -> geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties]:
    return geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
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
    general_uuid, default_aoi_feature_geojson_pydantic, default_augmented_artifact, default_info
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
        artifacts=[default_augmented_artifact],
        plugin_info=PluginBaseInfo(id=default_info.id, version=default_info.version),
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
def celery_worker_parameters():
    return {'hostname': 'test_plugin@hostname'}


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
    alembic_runner.migrate_up_to('head')
    return db_with_postgis


@pytest.fixture
def default_backend_db(db_with_tables) -> BackendDatabase:
    return BackendDatabase(connection_string=db_with_tables, user_agent='Test Climatoology Backend')


@pytest.fixture
def backend_with_computation_registered(
    default_backend_db, default_computation_info, default_info_final, set_basic_envs, frozen_time
) -> BackendDatabase:
    default_backend_db.write_info(info=default_info_final)
    default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_id=default_computation_info.plugin_info.id,
        plugin_version=default_computation_info.plugin_info.version,
        computation_shelf_life=default_info_final.computation_shelf_life,
    )
    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info.correlation_uuid, params=default_computation_info.params
    )
    with Session(default_backend_db.engine) as session:
        session.execute(
            insert(CeleryTaskMeta).values(
                id='1', task_id=default_computation_info.correlation_uuid, status=ComputationState.PENDING.value
            )
        )
        session.commit()
    return default_backend_db


@pytest.fixture
def backend_with_computation_successful(backend_with_computation_registered, default_computation_info):
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(
            update(CeleryTaskMeta)
            .values(status=ComputationState.SUCCESS.value)
            .where(CeleryTaskMeta.task_id == cast(default_computation_info.correlation_uuid, String))
        )
        session.execute(update(ComputationTable).values(valid_until=datetime.now() + timedelta(hours=12)))
        session.commit()
    backend_with_computation_registered.update_successful_computation(computation_info=default_computation_info)
    return backend_with_computation_registered


@pytest.fixture
def alembic_config() -> Config:
    return Config(config_options={'script_location': 'climatoology/store/database/migration'})


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
