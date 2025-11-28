import subprocess

from pytest_alembic.tests import (  # noqa: F401 don't remove these unused imports, they assure that the basic default
    # alembic tests are run
    test_model_definitions_match_ddl,
    test_single_head_revision,
    test_up_down_consistency,
    test_upgrade,
)
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session


def test_offline_migration_from_cli():
    completed_process = subprocess.run(['alembic', 'upgrade', '3d4313578291', '--sql'], capture_output=True)
    assert completed_process.returncode == 0
    output = completed_process.stdout.decode()
    assert 'CREATE TABLE info (\n' in output


def test_online_migration_from_cli(monkeypatch, db_connection_params, db_with_postgis):
    # Mimic having an env file or setting the env vars
    monkeypatch.setenv('postgres_host', db_connection_params['host'])
    monkeypatch.setenv('postgres_port', str(db_connection_params['port']))
    monkeypatch.setenv('postgres_database', db_connection_params['database'])
    monkeypatch.setenv('postgres_user', db_connection_params['user'])
    monkeypatch.setenv('postgres_password', db_connection_params['password'])

    completed_process = subprocess.run(['alembic', 'upgrade', '3d4313578291'], capture_output=True)
    assert completed_process.returncode == 0, completed_process.stderr.decode()
    engine = create_engine(db_with_postgis)
    with engine.connect() as connection:
        assert engine.dialect.has_table(connection, 'info')


def test_authors_link_preserved(alembic_runner, alembic_engine):
    alembic_runner.migrate_up_before('9ba5a3807edb')
    with Session(alembic_engine) as session:
        author_id = session.execute(
            text("select author_id from ca_base.author_info_link_table where info_id='prefilled_test_plugin'")
        ).scalar_one()
    assert author_id == 'Waldemar'

    alembic_runner.migrate_up_one()
    with Session(alembic_engine) as session:
        author_id = session.execute(
            text("select author_id from ca_base.author_info_link_table where info_key='prefilled_test_plugin;1.0.0'")
        ).scalar_one()
    assert author_id == 'Waldemar'

    alembic_runner.migrate_down_one()
    with Session(alembic_engine) as session:
        author_id = session.execute(
            text("select author_id from ca_base.author_info_link_table where info_id='prefilled_test_plugin'")
        ).scalar_one()
    assert author_id == 'Waldemar'
