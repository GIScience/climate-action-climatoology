from datetime import UTC, datetime
from typing import Generator

import pytest
from freezegun import freeze_time
from moto import mock_aws

from climatoology.app.settings import CABaseSettings
from climatoology.base.i18n import set_language
from climatoology.store.object_store import S3Storage
from climatoology.test import FIXTURE_RESOURCES_DIR


@pytest.fixture
def set_basic_envs(monkeypatch):
    monkeypatch.setenv('s3_host', 'test.host')
    monkeypatch.setenv('s3_port', '1234')
    monkeypatch.setenv('s3_access_key', 's3_test_key')
    monkeypatch.setenv('s3_secret_key', 's3_test_secret')
    monkeypatch.setenv('s3_bucket', 's3_test_bucket')

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
def set_to_german():
    set_language(lang='de', localisation_dir=FIXTURE_RESOURCES_DIR / 'locales')


@pytest.fixture
def frozen_time():
    with freeze_time(datetime(2018, 1, 1, 12, tzinfo=UTC), ignore=['celery']) as frozen_time:
        yield frozen_time


@pytest.fixture
def mocked_object_store(default_settings, monkeypatch) -> Generator[S3Storage]:
    # Set mock-related env vars
    monkeypatch.setenv('MOTO_S3_CUSTOM_ENDPOINTS', 'https://test.host:1234')
    monkeypatch.setenv('AWS_DEFAULT_REGION ', 'eu-central-1')

    with mock_aws():
        s3_storage = S3Storage(
            host=default_settings.s3_host,
            port=default_settings.s3_port,
            access_key=default_settings.s3_access_key,
            secret_key=default_settings.s3_secret_key,
            secure=True,
            bucket=default_settings.s3_bucket,
        )
        yield s3_storage
