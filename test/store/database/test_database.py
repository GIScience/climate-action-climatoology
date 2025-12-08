import uuid
from datetime import timedelta

import pytest
from semver import Version

from climatoology.base.computation import AoiProperties
from climatoology.base.plugin_info import PluginAuthor
from climatoology.store.database.database import BackendDatabase


def test_info_to_db_and_back(default_backend_db, default_plugin_info_final, default_plugin_key):
    plugin_key = default_backend_db.write_info(info=default_plugin_info_final)
    assert plugin_key == default_plugin_key

    read_info = default_backend_db.read_info(plugin_id=default_plugin_info_final.id)
    assert read_info == default_plugin_info_final


def test_get_info_key(backend_with_computation_registered):
    key = backend_with_computation_registered.read_info_key(plugin_id='test_plugin', plugin_version=Version(3, 1, 0))
    assert key == 'test_plugin;3.1.0'

    key = backend_with_computation_registered.read_info_key(plugin_id='test_plugin')
    assert key == 'test_plugin;3.1.0'

    key = backend_with_computation_registered.read_info_key(plugin_id='test_plugin', plugin_version=Version(99, 1, 0))
    assert key is None

    key = backend_with_computation_registered.read_info_key(plugin_id='not_a_plugin')
    assert key is None


def test_info_is_recreated(default_plugin_info_final, default_backend_db):
    # In devel mode we need to rewrite the info to the database (and we actually do this in all states)
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    changed_devel_info = default_plugin_info_final.model_copy(deep=True)
    changed_devel_info.authors = [
        PluginAuthor(
            name='Adam',
        ),
        PluginAuthor(
            name='Ddam',
        ),
        PluginAuthor(
            name='Bdam',
        ),
    ]
    changed_devel_info.methodology = 'Other methods'
    _ = default_backend_db.write_info(info=changed_devel_info)

    read_info = default_backend_db.read_info(plugin_id=changed_devel_info.id, plugin_version=changed_devel_info.version)
    assert read_info == changed_devel_info


def test_author_order_is_preserved(default_backend_db, default_plugin_info_final):
    default_info = default_plugin_info_final.model_copy(deep=True)
    default_info.authors = [
        PluginAuthor(
            name='Adam',
        ),
        PluginAuthor(
            name='Bdam',
        ),
        PluginAuthor(
            name='Cdam',
        ),
    ]
    _ = default_backend_db.write_info(info=default_info)

    info = default_plugin_info_final.model_copy(deep=True)
    info.authors = [
        PluginAuthor(
            name='Adam',
        ),
        PluginAuthor(
            name='Ddam',
        ),
        PluginAuthor(
            name='Bdam',
        ),
    ]
    _ = default_backend_db.write_info(info=info)

    read_info = default_backend_db.read_info(plugin_id=info.id)
    author_names = ['Adam', 'Ddam', 'Bdam']
    for i in range(0, len(author_names)):
        assert author_names[i] == read_info.authors[i].name


def test_read_info_latest_version_by_default(default_backend_db, default_plugin_info_final):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    newer_plugin_info = default_plugin_info_final.model_copy(deep=True)
    newer_plugin_info.version = Version(3, 2, 0)
    _ = default_backend_db.write_info(info=newer_plugin_info)

    read_info = default_backend_db.read_info(plugin_id=default_plugin_info_final.id)
    assert read_info == newer_plugin_info


def test_read_info_latest_version_by_default_with_commit_hash(default_backend_db, default_plugin_info_final):
    default_info = default_plugin_info_final.model_copy(deep=True)
    default_info.version = Version(1, 0, 0, build='zz')
    _ = default_backend_db.write_info(info=default_info)

    newer_plugin_info = default_info.model_copy(deep=True)
    newer_plugin_info.version = Version(1, 0, 0, build='aa')
    _ = default_backend_db.write_info(info=newer_plugin_info)

    read_info = default_backend_db.read_info(plugin_id=newer_plugin_info.id)
    assert str(read_info.version) == str(newer_plugin_info.version)


def test_read_info_older_version_if_requested(default_backend_db, default_plugin_info_final):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    newer_plugin_info = default_plugin_info_final.model_copy(deep=True)
    newer_plugin_info.version = Version(3, 2, 0)
    _ = default_backend_db.write_info(info=newer_plugin_info)

    read_info = default_backend_db.read_info(
        plugin_id=default_plugin_info_final.id, plugin_version=default_plugin_info_final.version
    )
    assert read_info == default_plugin_info_final


def test_add_authors(default_backend_db, default_plugin_info_final):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    changed_plugin_info = default_plugin_info_final
    changed_plugin_info.authors.append(PluginAuthor(name='anotherone'))
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    read_info = default_backend_db.read_info(plugin_id=default_plugin_info_final.id)
    assert read_info == changed_plugin_info


def test_add_authors_linked_to_plugin_versions(default_backend_db, default_plugin_info_final):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    changed_plugin_info = default_plugin_info_final.model_copy(deep=True)
    changed_plugin_info.version = Version(4, 1, 0)
    changed_plugin_info.authors = [PluginAuthor(name='different person')]
    _ = default_backend_db.write_info(info=changed_plugin_info)

    read_first_info = default_backend_db.read_info(
        plugin_id=default_plugin_info_final.id, plugin_version=default_plugin_info_final.version
    )
    assert read_first_info.authors == default_plugin_info_final.authors

    read_changed_info = default_backend_db.read_info(
        plugin_id=changed_plugin_info.id, plugin_version=changed_plugin_info.version
    )
    assert read_changed_info.authors == changed_plugin_info.authors


@pytest.mark.parametrize(
    'shelf_life,params,expected_deduplication',
    [
        (None, [{'same': True}, {'same': True}], True),
        (None, [{'different': True}, {'different': False}], False),
        (timedelta(0), [{'same': True}, {'same': True}], False),
        (timedelta(0), [{'different': True}, {'different': False}], False),
        (timedelta(days=7), [{'same': True}, {'same': True}], True),
        (timedelta(days=7), [{'different': True}, {'different': False}], False),
        (timedelta(seconds=1), [{'same': True}, {'same': True}], False),
        (timedelta(seconds=1), [{'different': True}, {'different': False}], False),
    ],
)
def test_register_computations(
    default_plugin,
    default_plugin_key,
    default_backend_db,
    default_computation_info,
    shelf_life,
    params,
    expected_deduplication,
    frozen_time,
):
    first_correlation_uuid = default_computation_info.correlation_uuid
    second_correlation_uuid = uuid.uuid4()

    db_correlation_uuid_original = default_backend_db.register_computation(
        correlation_uuid=first_correlation_uuid,
        requested_params=params[0],
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=shelf_life,
    )
    frozen_time.tick(delta=timedelta(days=1))
    db_correlation_uuid_duplicate = default_backend_db.register_computation(
        correlation_uuid=second_correlation_uuid,
        requested_params=params[1],
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=shelf_life,
    )
    deduplicated = db_correlation_uuid_original == db_correlation_uuid_duplicate
    assert deduplicated == expected_deduplication


def test_register_computation_retains_aoi_properties_when_read(
    backend_with_computation_registered, default_plugin_key, default_aoi_feature_geojson_pydantic
):
    custom_aoi_feature = default_aoi_feature_geojson_pydantic.model_copy(deep=True)
    # deliberately calling AoiProperties with extra arguments
    # noinspection PyArgumentList
    custom_aoi_feature.properties = AoiProperties(name='test_aoi', id='test_aoi_id', foo='bar', hello='world')

    correlation_uuid = uuid.uuid4()
    _ = backend_with_computation_registered.register_computation(
        correlation_uuid=correlation_uuid,
        requested_params={'id': '1'},
        aoi=custom_aoi_feature,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )

    computation_info = backend_with_computation_registered.read_computation(correlation_uuid=correlation_uuid)
    assert computation_info.aoi.properties == custom_aoi_feature.properties


def test_read_duplicate_computation(
    default_plugin, default_plugin_key, default_backend_db, default_aoi_feature_geojson_pydantic, frozen_time
):
    first_computation_id = uuid.uuid4()
    second_computation_id = uuid.uuid4()
    params = {'id': 1}
    _ = default_backend_db.register_computation(
        correlation_uuid=second_computation_id,
        requested_params=params,
        aoi=default_aoi_feature_geojson_pydantic,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )
    _ = default_backend_db.register_computation(
        correlation_uuid=first_computation_id,
        requested_params=params,
        aoi=default_aoi_feature_geojson_pydantic,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )

    first_computation = default_backend_db.read_computation(correlation_uuid=second_computation_id)
    second_computation = default_backend_db.read_computation(correlation_uuid=first_computation_id)

    assert first_computation == second_computation


def test_list_artifacts(general_uuid, backend_with_computation_successful, default_computation_info):
    default_computation_info.artifacts[0].rank = 0
    artifacts = backend_with_computation_successful.list_artifacts(correlation_uuid=general_uuid)
    assert artifacts == default_computation_info.artifacts


def test_resolve_deduplicated_computation_id(
    default_plugin, default_backend_db, default_computation_info, default_plugin_info, default_plugin_key
):
    duplicate_computation_id = uuid.uuid4()
    expected_computation_id = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )
    _ = default_backend_db.register_computation(
        correlation_uuid=duplicate_computation_id,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )

    db_computation_id = default_backend_db.resolve_computation_id(user_correlation_uuid=duplicate_computation_id)

    assert expected_computation_id == default_computation_info.correlation_uuid
    assert db_computation_id == default_computation_info.correlation_uuid


def test_update_successful_computation_with_validated_params(
    default_plugin, default_backend_db, default_computation_info, default_plugin_info, default_plugin_key, frozen_time
):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_plugin_info.computation_shelf_life,
    )

    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info.correlation_uuid, params=default_computation_info.params
    )
    default_backend_db.update_successful_computation(computation_info=default_computation_info)

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)

    default_computation_info.artifacts[0].rank = 0
    assert db_computation == default_computation_info


def test_computation_artifact_order_is_preserved(
    backend_with_computation_registered, default_computation_info, default_artifact_enriched
):
    info = default_computation_info.model_copy(deep=True)
    second_artifact = default_artifact_enriched.model_copy(deep=True)
    second_artifact.rank = 1
    second_artifact.name = 'second'

    info.artifacts.append(second_artifact)
    backend_with_computation_registered.update_successful_computation(computation_info=info)

    db_computation = backend_with_computation_registered.read_computation(
        correlation_uuid=default_computation_info.correlation_uuid
    )
    artifact_names = ['test_name', 'second']
    for i in range(0, len(artifact_names)):
        assert artifact_names[i] == db_computation.artifacts[i].name
        assert db_computation.artifacts[i].rank == i


def test_update_failed_computation(default_plugin, default_backend_db, default_computation_info, default_plugin_key):
    _ = default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=None,
    )

    default_backend_db.update_failed_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        failure_message='Custom failure message',
        cache=False,
    )

    db_computation = default_backend_db.read_computation(correlation_uuid=default_computation_info.correlation_uuid)
    assert db_computation.cache_epoch is None
    assert db_computation.message == 'Custom failure message'


def test_outdated_db_refuses_startup(db_with_postgis, alembic_runner):
    alembic_runner.migrate_up_to('45b227b8bee7')

    BackendDatabase(connection_string=db_with_postgis, user_agent='Test Climatoology Backend')
    with pytest.raises(
        RuntimeError,
        match=r'The target database is not compatible with the expectations by '
        r'climatoology. Make sure to update your database e.g. by running the '
        r'alembic migration or contacting your admin.',
    ):
        BackendDatabase(
            connection_string=db_with_postgis, user_agent='Test Climatoology Backend', assert_db_status=True
        )
