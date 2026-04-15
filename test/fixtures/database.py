import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterator

import alembic
import psycopg
import pytest
from alembic.command import stamp
from celery.backends.database import TaskExtended
from celery.backends.database.session import ResultModelBase
from psycopg import Connection
from pydantic_extra_types.language_code import LanguageAlpha2
from pytest_postgresql import factories
from sqlalchemy import NullPool, String, cast, create_engine, insert, update
from sqlalchemy.orm import Session

from climatoology.base.computation import ComputationState
from climatoology.store.database import migration
from climatoology.store.database.database import BackendDatabase
from climatoology.store.database.models.base import ClimatoologyTableBase
from climatoology.store.database.models.computation import ComputationTable
from climatoology.store.database.models.views import (
    ArtifactErrorsView,
    ComputationsSummaryView,
    FailedComputationsView,
    UsageView,
    ValidComputationsView,
    create_view_tracking_object,
)
from test.conftest import connection_to_string


def load_basics(**kwargs) -> None:
    with psycopg.connect(**kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE EXTENSION IF NOT EXISTS postgis;')


def load_tables(host: str, port: int, dbname: str, user: str, password: str) -> None:
    connection_str = f'postgresql+psycopg://{user}:{password}@{host}:{port}/{dbname}'
    engine = create_engine(connection_str, echo=False, poolclass=NullPool)

    with psycopg.connect(host=host, port=port, dbname=dbname, user=user, password=password) as conn:
        with conn.cursor() as cur:
            cur.execute('CREATE SCHEMA ca_base;')

    ResultModelBase.metadata.create_all(engine)
    ClimatoologyTableBase.metadata.create_all(engine)

    with engine.connect() as conn:
        conn.execute(create_view_tracking_object(ValidComputationsView).to_sql_statement_create())
        conn.execute(create_view_tracking_object(ComputationsSummaryView).to_sql_statement_create())
        conn.execute(create_view_tracking_object(UsageView).to_sql_statement_create())
        conn.execute(create_view_tracking_object(FailedComputationsView).to_sql_statement_create())
        conn.execute(create_view_tracking_object(ArtifactErrorsView).to_sql_statement_create())
        conn.commit()

    alembic_cfg = alembic.config.Config()
    alembic_cfg.set_main_option('script_location', str(Path(migration.__file__).parent))
    alembic_cfg.attributes['connection'] = engine

    stamp(config=alembic_cfg, revision='head')


if os.getenv('CI', 'False').lower() == 'true':
    db_template_basic = factories.postgresql_noproc(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        load=[load_basics],
    )
else:
    db_template_basic = factories.postgresql_proc(load=[load_basics])

db_template_with_tables = factories.postgresql_noproc(depends_on='db_template_basic', load=[load_tables])

db_fixture_basic = factories.postgresql('db_template_basic')
db_fixture_with_tables = factories.postgresql('db_template_with_tables')


@pytest.fixture
def default_backend_db(db_fixture_with_tables: Connection) -> Iterator[BackendDatabase]:
    connection_str = connection_to_string(db_fixture_with_tables)
    db = BackendDatabase(connection_string=connection_str, user_agent='Test Climatoology Backend')
    yield db


@pytest.fixture
def backend_with_computation_registered(
    default_backend_db,
    default_computation_info,
    default_computation_info_de,
    default_plugin_info_final,
    default_plugin_info_final_de,
    default_plugin_key,
    frozen_time,
) -> BackendDatabase:
    # Default is EN
    default_backend_db.write_info(info=default_plugin_info_final)
    default_backend_db.register_computation(
        correlation_uuid=default_computation_info.correlation_uuid,
        requested_params=default_computation_info.requested_params,
        aoi=default_computation_info.aoi,
        plugin_key=default_plugin_key,
        computation_shelf_life=default_plugin_info_final.computation_shelf_life,
    )
    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info.correlation_uuid, params=default_computation_info.params
    )

    # Add another computation in DE
    default_backend_db.write_info(info=default_plugin_info_final_de)
    default_backend_db.register_computation(
        correlation_uuid=default_computation_info_de.correlation_uuid,
        requested_params=default_computation_info_de.requested_params,
        aoi=default_computation_info_de.aoi,
        language=LanguageAlpha2('de'),
        plugin_key='test_plugin-3.1.0-de',
        computation_shelf_life=default_plugin_info_final_de.computation_shelf_life,
    )
    default_backend_db.add_validated_params(
        correlation_uuid=default_computation_info_de.correlation_uuid, params=default_computation_info_de.params
    )

    with Session(default_backend_db.engine) as session:
        session.execute(
            insert(TaskExtended).values(
                id='1', task_id=default_computation_info.correlation_uuid, status=ComputationState.PENDING
            )
        )
        session.execute(
            insert(TaskExtended).values(
                id='2', task_id=default_computation_info_de.correlation_uuid, status=ComputationState.PENDING
            )
        )
        session.commit()

    return default_backend_db


@pytest.fixture
def backend_with_computation_successful(backend_with_computation_registered, default_computation_info):
    with Session(backend_with_computation_registered.engine) as session:
        session.execute(
            update(TaskExtended)
            .values(status=ComputationState.SUCCESS, date_done=datetime.now())
            .where(TaskExtended.task_id == cast(default_computation_info.correlation_uuid, String))
        )
        session.execute(update(ComputationTable).values(valid_until=datetime.now() + timedelta(hours=12)))
        session.commit()
    backend_with_computation_registered.update_successful_computation(computation_info=default_computation_info)
    return backend_with_computation_registered
