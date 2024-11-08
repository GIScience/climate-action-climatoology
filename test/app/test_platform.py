from time import sleep
from unittest.mock import patch

import pytest
from semver import Version

from climatoology.app.platform import CAPlatformConnection
from climatoology.utility.exception import ClimatoologyVersionMismatchException


def test_list_default_active_plugins(default_platform_connection, celery_worker):
    computed_plugins = default_platform_connection.list_active_plugins()
    assert computed_plugins == set()


def test_list_no_active_plugins(default_platform_connection):
    computed_plugins = default_platform_connection.list_active_plugins()
    assert computed_plugins == set()


def test_list_active_plugins(default_platform_connection, celery_worker, default_plugin):
    computed_plugins = default_platform_connection.list_active_plugins()

    expected_plugins = {celery_worker.hostname.split('@')[0]}

    assert computed_plugins == expected_plugins


def test_request_info(default_platform_connection, default_info, default_plugin, celery_worker):
    computed_info = default_platform_connection.request_info(plugin_id='test_plugin')

    assert celery_worker.stats()['total'].get('info') == 1
    assert computed_info == default_info


@patch('climatoology.__version__', Version(1, 0, 0))
def test_request_info_plugin_version_assert(default_platform_connection, default_info, default_plugin, celery_worker):
    with pytest.raises(ClimatoologyVersionMismatchException, match='Refusing to register plugin.*'):
        default_platform_connection.request_info(plugin_id='test_plugin')


def test_send_compute(default_platform_connection, default_plugin, celery_worker, general_uuid):
    assert default_platform_connection.send_compute_request(
        plugin_id='test_plugin',
        params={'id': 1, 'name': 'John Doe'},
        correlation_uuid=general_uuid,
    )

    # wait for the worker to process the task
    sleep(1)
    assert celery_worker.stats()['total'].get('compute') == 1


def test_extract_plugin_id():
    computed_plugin_id = CAPlatformConnection._extract_plugin_id('a@b')
    assert computed_plugin_id == 'a'
