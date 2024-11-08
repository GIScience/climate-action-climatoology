import uuid
from pathlib import Path
from typing import List
from unittest.mock import patch, Mock

import pytest
import responses
from celery import Celery
from celery.utils.threads import LocalStack
from pydantic import BaseModel, Field
from semver import Version

import climatoology
from climatoology.app.platform import CAPlatformConnection
from climatoology.app.plugin import _create_plugin, generate_plugin_name
from climatoology.app.settings import CABaseSettings
from climatoology.app.tasks import CAPlatformComputeTask, CAPlatformInfoTask
from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.computation import ComputationResources, ComputationScope
from climatoology.base.info import Concern, PluginAuthor, _Info, generate_plugin_info
from climatoology.base.operator import Operator
from climatoology.store.object_store import MinioStorage
from climatoology.utility.api import HealthCheck

pytest_plugins = ('celery.contrib.pytest',)


@pytest.fixture
def set_basic_envs(monkeypatch):
    monkeypatch.setenv('minio_host', 'test_host')
    monkeypatch.setenv('minio_port', '1234')
    monkeypatch.setenv('minio_access_key', 'test_key')
    monkeypatch.setenv('minio_secret_key', 'test_secret')
    monkeypatch.setenv('minio_bucket', 'test_bucket')

    monkeypatch.setenv('rabbitmq_host', 'test_host')
    monkeypatch.setenv('rabbitmq_port', '1234')
    monkeypatch.setenv('rabbitmq_user', 'test_user')
    monkeypatch.setenv('rabbitmq_password', 'test_pw')

    monkeypatch.setenv('postgres_host', 'test_host')
    monkeypatch.setenv('postgres_port', '1234')
    monkeypatch.setenv('postgres_user', 'test_user')
    monkeypatch.setenv('postgres_password', 'test_password')
    monkeypatch.setenv('postgres_database', 'test_database')


@pytest.fixture
def default_settings(set_basic_envs) -> CABaseSettings:
    return CABaseSettings()


@pytest.fixture
def general_uuid() -> uuid.UUID:
    return uuid.uuid4()


@pytest.fixture
def default_info() -> _Info:
    info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent / 'resources/test.bib',
    )
    info.library_version = str(climatoology.__version__)
    info.operator_schema = {
        'properties': {
            'id': {'description': 'A required integer parameter.', 'examples': [1], 'title': 'ID', 'type': 'integer'},
            'name': {
                'default': 'John Doe',
                'description': 'An optional name parameter.',
                'examples': ['John Doe'],
                'title': 'Name',
                'type': 'string',
            },
        },
        'required': ['id'],
        'title': 'TestModel',
        'type': 'object',
    }
    return info


@pytest.fixture
def default_artifact(general_uuid):
    return _Artifact(
        name='test_name',
        modality=ArtifactModality.MAP_LAYER_GEOJSON,
        file_path=Path(__file__).parent / 'test_file.tiff',
        summary='Test summary',
        description='Test description',
        correlation_uuid=general_uuid,
        store_id=f'{general_uuid}_test_file.tiff',
    )


@pytest.fixture
def default_operator(default_info, default_artifact):
    class TestModel(BaseModel):
        id: int = Field(title='ID', description='A required integer parameter.', examples=[1])
        name: str = Field(
            title='Name', description='An optional name parameter.', examples=['John Doe'], default='John Doe'
        )

    class TestOperator(Operator[TestModel]):
        def info(self) -> _Info:
            return default_info.model_copy()

        def compute(self, resources: ComputationResources, params: TestModel) -> List[_Artifact]:
            return [default_artifact]

    yield TestOperator()


@pytest.fixture
def default_plugin(celery_app, celery_worker, default_operator, default_settings, mocked_object_store) -> Celery:
    with patch('climatoology.app.plugin.Celery', return_value=celery_app):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        celery_worker.reload()
        yield plugin


@pytest.fixture
def default_computation_resources(general_uuid) -> ComputationResources:
    with ComputationScope(general_uuid) as resources:
        yield resources


@pytest.fixture
def mocked_utility_response():
    with responses.RequestsMock() as rsps:
        rsps.get('http://localhost:80/health', json=HealthCheck().model_dump())
        yield rsps


@pytest.fixture
def mocked_object_store() -> dict:
    with patch('climatoology.store.object_store.Minio') as minio_client:
        minio_storage = MinioStorage(
            host='minio.test.org',
            port=9999,
            access_key='key',
            secret_key='secret',
            secure=True,
            bucket='test_bucket',
        )
        yield {'minio_storage': minio_storage, 'minio_client': minio_client}


@pytest.fixture
def default_computation_task(default_operator, mocked_object_store, general_uuid) -> CAPlatformComputeTask:
    compute_task = CAPlatformComputeTask(operator=default_operator, object_store=mocked_object_store['minio_storage'])
    request = Mock()
    request.correlation_id = general_uuid
    compute_task.request_stack = LocalStack()
    compute_task.request_stack.push(request)
    return compute_task


@pytest.fixture
def default_info_task(default_operator, general_uuid) -> CAPlatformInfoTask:
    info_task = CAPlatformInfoTask(operator=default_operator)
    request = Mock()
    request.correlation_id = general_uuid
    info_task.request_stack = LocalStack()
    info_task.request_stack.push(request)
    return info_task


@pytest.fixture
def default_platform_connection(celery_app) -> CAPlatformConnection:
    with patch('climatoology.app.platform.CAPlatformConnection.construct_celery_app', return_value=celery_app):
        yield CAPlatformConnection()


@pytest.fixture
def celery_config():
    return {'worker_direct': True}


@pytest.fixture
def patch_pytest_celery_worker_hostname():
    # This is required due to https://github.com/celery/celery/issues/9404
    with patch('celery.contrib.testing.worker.anon_nodename', return_value=generate_plugin_name('test_plugin')) as p:
        yield p


@pytest.fixture
def celery_worker(patch_pytest_celery_worker_hostname, celery_worker):
    return celery_worker
