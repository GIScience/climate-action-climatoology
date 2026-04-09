import subprocess

import pytest
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
from test.fixtures.database import connection_to_string


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
