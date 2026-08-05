import subprocess
from datetime import datetime

import pytest
import shapely
from pytest_alembic.tests import (  # noqa: F401 don't remove these unused imports, they assure that the basic default
    # alembic tests are run
    test_model_definitions_match_ddl,
    test_single_head_revision,
    test_up_down_consistency,
    test_upgrade,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from climatoology.store.database.database import BackendDatabase
from climatoology.test.utils import connection_to_string


def test_assert_db_status(alembic_runner):
    BackendDatabase(
        connection_string=alembic_runner.connection_executor.connection.url, user_agent='Test Climatoology Backend'
    )
    with pytest.raises(
        RuntimeError, match=r'The target database is not compatible with the expectations by climatoology.*'
    ):
        BackendDatabase(
            connection_string=alembic_runner.connection_executor.connection.url,
            user_agent='Test Climatoology Backend',
            assert_db_status=True,
        )

    alembic_runner.migrate_up_to('head')
    db = BackendDatabase(
        connection_string=alembic_runner.connection_executor.connection.url,
        user_agent='Test Climatoology Backend',
        assert_db_status=True,
    )
    assert db


def test_offline_migration_from_cli():
    completed_process = subprocess.run(['alembic', 'upgrade', '3d4313578291', '--sql'], capture_output=True)
    assert completed_process.returncode == 0
    output = completed_process.stdout.decode()
    assert 'CREATE TABLE info (\n' in output


def test_online_migration_from_cli(monkeypatch, db_fixture_basic):
    # Mimic having an env file or setting the env vars
    monkeypatch.setenv('postgres_host', db_fixture_basic.info.host)
    monkeypatch.setenv('postgres_port', str(db_fixture_basic.info.port))
    monkeypatch.setenv('postgres_database', db_fixture_basic.info.dbname)
    monkeypatch.setenv('postgres_user', db_fixture_basic.info.user)
    monkeypatch.setenv('postgres_password', db_fixture_basic.info.password)

    connection_str = connection_to_string(db_fixture_basic)

    completed_process = subprocess.run(['alembic', 'upgrade', '3d4313578291'], capture_output=True)
    assert completed_process.returncode == 0, completed_process.stderr.decode()
    engine = create_engine(connection_str)
    with engine.connect() as connection:
        assert engine.dialect.has_table(connection, 'info')


def test_database_migration_values(default_plugin_info_final, alembic_runner, alembic_engine):
    """This is a convenient test for checking the actual values of migrated columns."""
    alembic_runner.migrate_up_to('head')
    with Session(alembic_engine) as session:
        plugin_key = (
            session.execute(text(f"select key from ca_base.plugin_info where id='{default_plugin_info_final.id}'"))
            .scalars()
            .all()
        )
    assert 'test_plugin-3.1.0-en' in plugin_key


def test_database_migrations_non_breaking(alembic_runner, alembic_engine):
    """
    Assert that database entries that were valid in v7.0.0 are still valid at 'head'. If these entries need to be
    updated, that indicates breaking changes in the migrations.
    """
    alembic_runner.migrate_up_to('head')

    # Plugin info, written on plugin startup
    plugin_info_item = {
        'id': 'plugin_v7',
        'version': '1.2.3',
        'name': 'plugin_v7',
        'repository': 'https://gitlab.heigit.org/climate-action/climatoology',
        'state': 'ACTIVE',
        'concerns': [],
        'teaser': 'A teaser',
        'purpose': 'My purpose',
        'methodology': 'A methodology',
        'demo_config': {
            'params': '{}',
            'name': 'Demo',
            'aoi': {
                'type': 'MultiPolygon',
                'coordinates': [[[[8.8, 49.4], [8.9, 49.4], [8.9, 49.5], [8.8, 49.5], [8.8, 49.4]]]],
            },
        },
        'assets': {'icon': 'assets/plugin_v7/latest/ICON.png'},
        'operator_schema': {'properties': {}, 'title': 'ComputeInput', 'type': 'object'},
        'library_version': '7.0.0',
        'latest': True,
    }

    plugin_author_item = {'name': 'Author_v7'}
    plugin_info_author_link_item = {
        'info_key': 'plugin_v7-1.2.3-en',  # the plugin_key changed since v7.0.0 to include language and use `-` instead of `;` as a separator, but this is a computed field, so it is non-breaking
        'author_id': 'Author_v7',
        'author_seat': 0,
    }

    alembic_runner.insert_into('ca_base.plugin_info', plugin_info_item)
    alembic_runner.insert_into('ca_base.plugin_author', plugin_author_item)
    alembic_runner.insert_into('ca_base.plugin_info_author_link', plugin_info_author_link_item)

    # Computations, registered by the gateway
    computation_uuid = '00000000-1111-2222-3333-444444444444'
    aoi_geom = shapely.to_wkt(
        shapely.MultiPolygon([[[[8.8, 49.4], [8.9, 49.4], [8.9, 49.5], [8.8, 49.5], [8.8, 49.4]]]])
    )
    computation_item = {
        'correlation_uuid': computation_uuid,
        'valid_until': datetime.now(),
        'requested_params': {},
        'aoi_geom': aoi_geom,
        'plugin_key': 'plugin_v7-1.2.3-en',  # the plugin_key changed since v7.0.0 to include language and use `-` instead of `;` as a separator, but this is a computed field, so it is non-breaking
        'artifact_errors': {},
        'language': 'en',  # language didn't exist in v7.0.0, but it is provided by the gateway, so is non-breaking for plugins
    }
    computation_lookup_item = {
        'user_correlation_uuid': computation_uuid,
        'request_ts': datetime.now(),
        'aoi_name': 'Test AOI',
        'aoi_id': '123',
        'is_demo': False,
        'computation_id': computation_uuid,
    }

    alembic_runner.insert_into('ca_base.computation', computation_item)
    alembic_runner.insert_into('ca_base.computation_lookup', computation_lookup_item)

    # Artifacts, inserted by the plugin compute task
    artifact_item = {
        'rank': 0,
        'correlation_uuid': computation_uuid,
        'name': 'Test artifact',
        'modality': 'MARKDOWN',
        'primary': True,
        'tags': set(),
        'summary': 'A summary',
        'attachments': {},
        'filename': 'filename.md',
    }
    alembic_runner.insert_into('ca_base.artifact', artifact_item)
