import pytest

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
