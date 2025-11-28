"""add-version-as-info-primary-key

Revision ID: 9ba5a3807edb
Revises: 0364d1c8dd4e
Create Date: 2025-11-11 14:13:50.707401

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '9ba5a3807edb'
down_revision: Union[str, None] = '540bd0182fc3'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""

    # Add new key to plugin info table
    op.add_column(
        'info',
        sa.Column(
            'key', sa.String(), sa.Computed("id::text || ';'::text || version::text"), nullable=False, primary_key=True
        ),
        schema='ca_base',
    )
    op.add_column('info', sa.Column('latest', sa.Boolean(), nullable=True), schema='ca_base')
    op.execute(sa.text('update ca_base.info set latest=true'))
    op.alter_column('info', 'latest', nullable=False, schema='ca_base')

    # Add new info key to other tables
    op.add_column('author_info_link_table', sa.Column('info_key', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.author_info_link_table set info_key=ca_base.info.key from ca_base.info where ca_base.info.id = info_id'
        )
    )
    op.alter_column('author_info_link_table', 'info_key', nullable=False, schema='ca_base')

    # Fix previous table definition
    op.create_primary_key(
        'author_info_link_table_pkey', 'author_info_link_table', ['info_key', 'author_id'], schema='ca_base'
    )
    op.execute(sa.text('update ca_base.author_info_link_table set author_seat=1 where author_seat is null'))
    op.alter_column('author_info_link_table', 'author_seat', nullable=False, schema='ca_base')

    op.add_column('computation', sa.Column('plugin_key', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.computation set plugin_key=ca_base.info.key from ca_base.info where ca_base.info.id = plugin_id'
        )
    )
    op.alter_column('computation', 'plugin_key', nullable=False, schema='ca_base')

    # Reset deduplication key
    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca_base')
    op.drop_column('computation', 'deduplication_key', schema='ca_base')
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.Uuid(),
            sa.Computed('md5(requested_params::text||st_astext(aoi_geom))::uuid'),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint',
        'computation',
        ['plugin_key', 'deduplication_key', 'cache_epoch'],
        schema='ca_base',
    )

    # Drop previous constraints
    op.drop_constraint(
        op.f('author_info_link_table_info_id_fkey'), 'author_info_link_table', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint(op.f('computation_plugin_id_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint('info_pkey', 'info', schema='ca_base', type_='primary')

    # Add replacement constraints
    op.create_primary_key('info_pkey', 'info', ['key'], schema='ca_base')
    op.create_foreign_key(
        None,
        'author_info_link_table',
        'info',
        ['info_key'],
        ['key'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )
    op.create_foreign_key(
        None, 'computation', 'info', ['plugin_key'], ['key'], source_schema='ca_base', referent_schema='ca_base'
    )

    # Drop old columns from tables
    op.drop_column('author_info_link_table', 'info_id', schema='ca_base')
    op.drop_column('computation', 'plugin_id', schema='ca_base')
    op.drop_column('computation', 'plugin_version', schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""

    # Add previous columns to tables
    op.add_column('computation', sa.Column('plugin_version', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.computation set plugin_version=ca_base.info.version from ca_base.info where ca_base.info.key=ca_base.computation.plugin_key'
        )
    )
    op.alter_column('computation', 'plugin_version', nullable=False, schema='ca_base')

    op.add_column('computation', sa.Column('plugin_id', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.computation set plugin_id=ca_base.info.id from ca_base.info where ca_base.info.key=ca_base.computation.plugin_key'
        )
    )
    op.alter_column('computation', 'plugin_id', nullable=False, schema='ca_base')

    op.add_column('author_info_link_table', sa.Column('info_id', sa.String(), nullable=True), schema='ca_base')
    op.execute(
        sa.text(
            'update ca_base.author_info_link_table set info_id=ca_base.info.id from ca_base.info where ca_base.info.key=ca_base.author_info_link_table.info_key'
        )
    )
    op.alter_column('author_info_link_table', 'info_id', nullable=False, schema='ca_base')

    # Drop newer constraints
    op.drop_constraint(op.f('computation_plugin_key_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint(
        op.f('author_info_link_table_info_key_fkey'), 'author_info_link_table', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint('info_pkey', 'info', schema='ca_base', type_='primary')

    # Add previous constraints
    op.create_primary_key('info_pkey', 'info', ['id'], schema='ca_base')
    op.create_foreign_key(
        None, 'author_info_link_table', 'info', ['info_id'], ['id'], source_schema='ca_base', referent_schema='ca_base'
    )
    op.create_foreign_key(
        None, 'computation', 'info', ['plugin_id'], ['id'], source_schema='ca_base', referent_schema='ca_base'
    )

    # Reset deduplication key
    op.drop_constraint('computation_deduplication_constraint', 'computation', schema='ca_base')
    op.drop_column('computation', 'deduplication_key', schema='ca_base')
    op.add_column(
        'computation',
        sa.Column(
            'deduplication_key',
            sa.Uuid(),
            sa.Computed(
                'md5(requested_params::text||st_astext(aoi_geom)||plugin_id::text||plugin_version::text)::uuid'
            ),
            nullable=False,
        ),
        schema='ca_base',
    )
    op.create_unique_constraint(
        'computation_deduplication_constraint', 'computation', ['deduplication_key', 'cache_epoch'], schema='ca_base'
    )

    # Undo table fixes
    op.alter_column('author_info_link_table', 'author_seat', nullable=True, schema='ca_base')
    op.drop_constraint('author_info_link_table_pkey', 'author_info_link_table', schema='ca_base', type_='primary')

    # Drop info key from related tables
    op.drop_column('computation', 'plugin_key', schema='ca_base')
    op.execute(
        sa.text(
            'delete from ca_base.author_info_link_table where info_key in (select key from ca_base.info where not latest)'
        )
    )
    op.drop_column('author_info_link_table', 'info_key', schema='ca_base')

    # Drop key from info table
    op.execute(sa.text('delete from ca_base.info where latest = FALSE'))
    op.drop_column('info', 'key', schema='ca_base')
    op.drop_column('info', 'latest', schema='ca_base')
