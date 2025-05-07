import datetime
import uuid
from unittest.mock import patch

import pytest

from climatoology.base.event import ComputationState
from climatoology.base.info import PluginAuthor
from climatoology.utility.exception import VersionMismatchException


def test_info_to_db_and_back(default_backend_db, default_info_final):
    plugin_id = default_backend_db.write_info(info=default_info_final)
    assert plugin_id == default_info_final.plugin_id

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == default_info_final


def test_write_info_abort_if_older_version(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    older_plugin_info = default_info_final
    older_plugin_info.version = '2.1.0'
    with pytest.raises(VersionMismatchException, match=r'Refusing to register plugin*'):
        _ = default_backend_db.write_info(info=older_plugin_info)


def test_write_info_downgrade_older_version(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    older_plugin_info = default_info_final
    older_plugin_info.version = '2.1.0'
    _ = default_backend_db.write_info(info=default_info_final, revert=True)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == older_plugin_info


def test_update_info_equal_version(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    changed_plugin_info = default_info_final
    changed_plugin_info.methodology = 'other methods'
    _ = default_backend_db.write_info(info=default_info_final, revert=True)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == changed_plugin_info


def test_update_info_newer_version(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    newer_plugin_info = default_info_final
    newer_plugin_info.version = '3.2.0'
    _ = default_backend_db.write_info(info=default_info_final, revert=True)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == newer_plugin_info


def test_add_authors(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    changed_plugin_info = default_info_final
    changed_plugin_info.authors.append(PluginAuthor(name='anotherone'))
    _ = default_backend_db.write_info(info=default_info_final, revert=True)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == changed_plugin_info


def test_remove_authors(default_backend_db, default_info_final):
    _ = default_backend_db.write_info(info=default_info_final)

    changed_plugin_info = default_info_final
    changed_plugin_info.authors = [PluginAuthor(name='anotherone')]
    _ = default_backend_db.write_info(info=default_info_final, revert=True)

    read_info = default_backend_db.read_info(plugin_id=default_info_final.plugin_id)
    assert read_info == changed_plugin_info


def test_computation_to_db_and_back(default_plugin, default_backend_db, default_computation_info):
    with patch('climatoology.store.database.database.datetime', wraps=datetime.datetime) as dt_mock:
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        db_correlation_uuid = default_backend_db.register_computation(
            correlation_uuid=default_computation_info.correlation_uuid,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
    assert db_correlation_uuid == default_computation_info.correlation_uuid

    db_computation = default_backend_db.read_computation(correlation_uuid=db_correlation_uuid)

    # Setting the following because they differ between the default fixture and the basic upload
    default_computation_info.artifacts = []
    default_computation_info.status = ComputationState.PENDING

    assert db_computation == default_computation_info


def test_register_duplicate_computation(default_plugin, default_backend_db, default_computation_info):
    with patch('climatoology.store.database.database.datetime', wraps=datetime.datetime) as dt_mock:
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        db_correlation_uuid_original = default_backend_db.register_computation(
            correlation_uuid=default_computation_info.correlation_uuid,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
        db_correlation_uuid_duplicate = default_backend_db.register_computation(
            correlation_uuid=uuid.uuid4(),
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
    assert db_correlation_uuid_original == db_correlation_uuid_duplicate


def test_register_different_computation(default_plugin, default_backend_db, default_computation_info):
    with patch('climatoology.store.database.database.datetime', wraps=datetime.datetime) as dt_mock:
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        db_correlation_uuid_original = default_backend_db.register_computation(
            correlation_uuid=default_computation_info.correlation_uuid,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
        db_correlation_uuid_duplicate = default_backend_db.register_computation(
            correlation_uuid=uuid.uuid4(),
            params={'other': 'param'},
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
    assert db_correlation_uuid_original != db_correlation_uuid_duplicate


def test_read_duplicate_computation(default_plugin, default_backend_db, default_computation_info):
    duplicate_computation_id = uuid.uuid4()
    with patch('climatoology.store.database.database.datetime', wraps=datetime.datetime) as dt_mock:
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        _ = default_backend_db.register_computation(
            correlation_uuid=default_computation_info.correlation_uuid,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
        _ = default_backend_db.register_computation(
            correlation_uuid=duplicate_computation_id,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )

    db_computation = default_backend_db.read_computation(correlation_uuid=duplicate_computation_id)

    # Setting the following because they differ between the default fixture and the basic upload
    default_computation_info.artifacts = []
    default_computation_info.status = ComputationState.PENDING

    assert db_computation == default_computation_info


def test_resolve_computation_id(default_plugin, default_backend_db, default_computation_info):
    duplicate_computation_id = uuid.uuid4()
    with patch('climatoology.store.database.database.datetime', wraps=datetime.datetime) as dt_mock:
        dt_mock.now.return_value = datetime.datetime(day=1, month=1, year=2025)
        _ = default_backend_db.register_computation(
            correlation_uuid=default_computation_info.correlation_uuid,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )
        _ = default_backend_db.register_computation(
            correlation_uuid=duplicate_computation_id,
            params=default_computation_info.params,
            aoi=default_computation_info.aoi,
            plugin_id=default_computation_info.plugin_info.plugin_id,
            plugin_version=default_computation_info.plugin_info.plugin_version,
        )

    db_computation_id = default_backend_db.resolve_computation_id(user_correlation_uuid=duplicate_computation_id)

    assert db_computation_id == default_computation_info.correlation_uuid


def test_update_successful_computation(default_plugin, default_backend_db, default_computation_info):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_id=default_computation_info.plugin_info.plugin_id,
        plugin_version=default_computation_info.plugin_info.plugin_version,
    )

    default_backend_db.update_successful_computation(computation_info=default_computation_info)

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)
    assert db_computation == default_computation_info


def test_update_failed_computation(default_plugin, default_backend_db, default_computation_info):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_id=default_computation_info.plugin_info.plugin_id,
        plugin_version=default_computation_info.plugin_info.plugin_version,
    )

    default_backend_db.update_failed_computation(
        correlation_uuid=str(default_computation_info.correlation_uuid), failure_message='Custom failure message'
    )

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)
    assert db_computation.status == ComputationState.FAILURE
    assert db_computation.message == 'Custom failure message'
