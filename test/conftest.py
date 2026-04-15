from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

import pytest
import responses
from freezegun import freeze_time
from psycopg import Connection
from pydantic import BaseModel, Field

from climatoology.app.settings import CABaseSettings
from climatoology.base.i18n import N_, set_language
from climatoology.store.object_store import MinioStorage
from climatoology.utility.api import HealthCheck

pytest_plugins = (
    'celery.contrib.pytest',
    'test.fixtures.alembic',
    'test.fixtures.aoi',
    'test.fixtures.artifact',
    'test.fixtures.computation',
    'test.fixtures.database',
    'test.fixtures.plugin',
    'test.fixtures.plugin_info',
    'test.fixtures.naturalness',
)

TEST_RESOURCES_DIR = Path(__file__).parent / 'resources'


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
def set_to_german():
    set_language(lang='de', localisation_dir=TEST_RESOURCES_DIR / 'locales')


@pytest.fixture
def frozen_time():
    with freeze_time(datetime(2018, 1, 1, 12, tzinfo=UTC), ignore=['celery']) as frozen_time:
        yield frozen_time


class Option(StrEnum):
    OPT1 = 'OPT1'
    OPT2 = 'OPT2'


class Mapping(BaseModel):
    key: str = 'value'


class TestModel(BaseModel):
    __test__ = False
    id: int = Field(title=N_('ID'), description=N_('A required integer parameter.'), examples=[1])
    execution_time: float = Field(
        title=N_('Execution time'),
        description=N_('The time for the compute to run (in seconds)'),
        examples=[10.0],
        default=0.0,
    )
    name: str = Field(
        title=N_('Name'), description=N_('An optional name parameter.'), examples=['John Doe'], default='John Doe'
    )
    option: Option = Option.OPT1
    mapping: Mapping = Mapping()


def connection_to_string(connection: Connection) -> str:
    user = connection.info.user
    password = connection.info.password
    host = connection.info.host
    port = connection.info.port
    dbname = connection.info.dbname

    connection_str = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}'

    return connection_str
