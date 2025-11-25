from typing import Optional, Type

from alembic import context
from alembic_utils.pg_view import PGView
from alembic_utils.replaceable_entity import register_entities
from celery.backends.database.session import ResultModelBase
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import engine_from_config, pool
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.schema import SchemaItem
from sqlalchemy_utils.view import CreateView

from climatoology.store.database.models.base import ClimatoologyTableBase
from climatoology.store.database.models.views import (
    ArtifactErrorsView,
    ComputationsSummaryView,
    FailedComputationsView,
    UsageView,
    ValidComputationsView,
)


def create_view_tracking_object(view_cls: Type[ClimatoologyTableBase]) -> PGView:
    """
    Create an alembic tracking object for a view, so changes to the view are recorded.

    The workaround is required because PGView does not support creating view from `select()` statements and the two
    libraries (sqlalchemy-utils, used for view creation, and alembic-utils, used for view tracking) are not compatible.

    :param view_cls: the view class to create a tracking object for
    :return: the tracking object
    """
    select_stmt = CreateView(view_cls.__table__.fullname, view_cls.select_statement)
    select_stmt = select_stmt.compile(dialect=postgresql.dialect())
    select_stmt = str(select_stmt).replace(f'CREATE VIEW {view_cls.__table__.fullname} AS ', '')
    tracking_object = PGView(
        schema=view_cls.__table__.schema,
        signature=view_cls.__table__.name,
        definition=select_stmt,
    )
    return tracking_object


register_entities(
    [
        create_view_tracking_object(ValidComputationsView),
        create_view_tracking_object(ComputationsSummaryView),
        create_view_tracking_object(UsageView),
        create_view_tracking_object(FailedComputationsView),
        create_view_tracking_object(ArtifactErrorsView),
    ]
)


class MigrationSettings(BaseSettings):
    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_user: str
    postgres_password: str

    model_config = SettingsConfigDict(env_file='.env.migration')

    @property
    def db_connection_string(self) -> str:
        return f'postgresql+psycopg://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_database}'


target_metadata = [ClimatoologyTableBase.metadata, ResultModelBase.metadata]


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_name='postgresql',
        dialect_opts={'paramstyle': 'named'},
        include_object=include_object,
        include_schemas=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # this function is adapted from the suggested one at
    # https://alembic.sqlalchemy.org/en/latest/cookbook.html#sharing-a-connection-across-one-or-more-programmatic-migration-commands
    # to accommodate the test library
    # the connectable (under the built-in 'connection'-attribute) is set during programmatic access
    connectable = context.config.attributes.get('connection', None)
    if connectable is None:
        # we require the user to set the settings via a .env file or via env variables
        # noinspection PyArgumentList
        settings = MigrationSettings()
        connectable = engine_from_config(
            configuration={},
            url=settings.db_connection_string,
            prefix='sqlalchemy.',
            poolclass=pool.NullPool,
        )

    with connectable.connect() as connection:
        context.configure(
            connection=connection, target_metadata=target_metadata, include_object=include_object, include_schemas=True
        )

        with context.begin_transaction():
            context.run_migrations()


def include_object(
    object_under_review: SchemaItem, name: Optional[str], type_: str, reflected: bool, compare_to: Optional[SchemaItem]
) -> bool:
    if type_ == 'table' and name in ['spatial_ref_sys'] and object_under_review.schema is None:
        return False
    if type_ == 'view' and object_under_review.schema == 'public':
        return False
    if type_ == 'grant_table':
        return False  # we decided to ignore table grants due to https://github.com/olirice/alembic_utils/issues/137
    if type_ == 'extension' and name == 'public.postgis':
        return False
    else:
        return True


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
