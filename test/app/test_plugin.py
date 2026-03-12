import re
from unittest.mock import patch

import pytest
from celery import Celery
from semver import Version
from sqlalchemy import select
from sqlalchemy.orm import Session

from climatoology.app.exception import VersionMismatchError
from climatoology.app.plugin import _create_plugin, _version_is_compatible, extract_plugin_id, synch_info
from climatoology.base.exception import InputValidationError
from climatoology.store.database.models.plugin_info import PluginInfoTable


def test_plugin_creation(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)
        assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        assert plugin.tasks.unregister('compute') is None


def test_worker_send_compute_task(
    default_plugin,
    default_computation_info,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }

    computed_compute_result = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    assert computed_compute_result == expected_computation_info


def test_successful_compute_saves_metadata_to_storage(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    default_computation_info,
    mocker,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)
    save_info_spy = mocker.spy(default_plugin.tasks['compute'], '_save_computation_info')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }
    _ = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    save_info_spy.assert_called_once_with(computation_info=expected_computation_info)


def test_successful_compute_saves_metadata_to_backend(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    default_computation_info,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)
    expected_computation_info.artifacts[0].rank = 0

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }
    _ = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    saved_computation = backend_with_computation_registered.read_computation(correlation_uuid=general_uuid)
    assert saved_computation == expected_computation_info


def test_failing_compute_updates_backend(
    default_plugin, general_uuid, default_aoi_feature_pure_dict, backend_with_computation_registered
):
    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {},
    }
    with pytest.raises(InputValidationError, match='ID: Field required. You provided: {}.'):
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    updated_computation = backend_with_computation_registered.read_computation(correlation_uuid=general_uuid)
    assert updated_computation.message == 'ID: Field required. You provided: {}.'


def test_version_matches_raises_on_lower(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    older_plugin_info = default_plugin_info_final
    older_plugin_info.version = Version(2, 1, 0)
    with pytest.raises(VersionMismatchError, match=r'Refusing to register plugin*'):
        _version_is_compatible(info=older_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_equal(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)
    assert _version_is_compatible(info=default_plugin_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_no_plugin_registered(default_backend_db, default_plugin_info_final, celery_app):
    assert _version_is_compatible(info=default_plugin_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_higher(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    newer_plugin_info = default_plugin_info_final
    newer_plugin_info.version = Version(3, 1, 1)
    assert _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_higher_not_alone(default_plugin, default_backend_db, default_plugin_info_final, celery_app):
    newer_plugin_info = default_plugin_info_final.model_copy(deep=True)
    newer_plugin_info.version = Version(3, 1, 1)
    with pytest.raises(
        AssertionError,
        match=re.escape(
            'Refusing to register plugin Test Plugin version 3.1.1 because a plugin with a lower version (3.1.0) is already running. Make sure to stop it before upgrading.',
        ),
    ):
        _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_extract_plugin_id():
    computed_plugin_id = extract_plugin_id('a@b')
    assert computed_plugin_id == 'a'


def test_synch_info_multiple_languages(default_backend_db, default_plugin_info_enriched, mocked_object_store):
    plugin_info_enriched = default_plugin_info_enriched.model_copy(deep=True)
    plugin_info_enriched.teaser.update({'de': 'Das ist ein Teaser auf Deutsch.'})
    plugin_info_enriched.purpose.update({'de': 'DE purpose'})
    plugin_info_enriched.methodology.update({'de': 'DE methodology'})

    synched_info = synch_info(info=plugin_info_enriched, db=default_backend_db, storage=mocked_object_store)

    assert synched_info.keys() == {'de', 'en'}

    with Session(default_backend_db.engine) as session:
        select_stmt = select(PluginInfoTable.language, PluginInfoTable.latest)
        infos = session.execute(select_stmt).all()

    assert infos == [('en', True), ('de', True)]
