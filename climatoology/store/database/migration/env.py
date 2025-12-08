from typing import Optional

from alembic import context
from alembic_utils.replaceable_entity import register_entities
from celery.backends.database.session import ResultModelBase
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import engine_from_config, pool
from sqlalchemy.sql.schema import SchemaItem

from climatoology.store.database.models.base import ClimatoologyTableBase
from climatoology.store.database.models.views import (
    ArtifactErrorsView,
    ComputationsSummaryView,
    FailedComputationsView,
    UsageView,
    ValidComputationsView,
    create_view_tracking_object,
)

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
    schema = getattr(object_under_review, 'schema', None)
    if type_ == 'table' and name in ['spatial_ref_sys'] and schema is None:
        return False
    if type_ == 'view' and schema == 'public':
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
