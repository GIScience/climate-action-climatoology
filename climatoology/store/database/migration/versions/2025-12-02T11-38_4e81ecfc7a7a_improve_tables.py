"""improve_tables

Revision ID: 4e81ecfc7a7a
Revises: 2d37688e948a
Create Date: 2025-11-27 11:38:24.901912

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '4e81ecfc7a7a'
down_revision: Union[str, None] = '2d37688e948a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint('artifact_correlation_uuid_fkey', 'artifact', schema='ca_base')
    op.drop_constraint('computation_lookup_computation_id_fkey', 'computation_lookup', schema='ca_base')
    op.alter_column(
        'artifact',
        'correlation_uuid',
        existing_type=sa.UUID(),
        type_=sa.String(),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'correlation_uuid',
        existing_type=sa.UUID(),
        type_=sa.String(),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'computation_lookup',
        'computation_id',
        existing_type=sa.UUID(),
        type_=sa.String(),
        existing_nullable=False,
        schema='ca_base',
    )
    op.create_foreign_key(
        None,
        'artifact',
        'computation',
        ['correlation_uuid'],
        ['correlation_uuid'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )
    op.create_foreign_key(
        None,
        'computation_lookup',
        'computation',
        ['computation_id'],
        ['correlation_uuid'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )

    op.alter_column(
        'artifact',
        'sources',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'attachments',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'artifact_errors',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'sources',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'demo_config',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'assets',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'operator_schema',
        existing_type=postgresql.JSON(astext_type=sa.Text()),
        type_=postgresql.JSONB(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )

    op.create_index(
        op.f('ix_ca_base_artifact_correlation_uuid'), 'artifact', ['correlation_uuid'], unique=False, schema='ca_base'
    )
    op.create_index(
        op.f('ix_ca_base_computation_plugin_key'), 'computation', ['plugin_key'], unique=False, schema='ca_base'
    )
    op.create_index(
        op.f('ix_ca_base_computation_valid_until'), 'computation', ['valid_until'], unique=False, schema='ca_base'
    )
    op.create_index(
        op.f('ix_ca_base_computation_lookup_aoi_id'), 'computation_lookup', ['aoi_id'], unique=False, schema='ca_base'
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_ca_base_artifact_correlation_uuid'), 'artifact', schema='ca_base')
    op.drop_index(op.f('ix_ca_base_computation_plugin_key'), 'computation', schema='ca_base')
    op.drop_index(op.f('ix_ca_base_computation_valid_until'), 'computation', schema='ca_base')
    op.drop_index(op.f('ix_ca_base_computation_lookup_aoi_id'), 'computation_lookup', schema='ca_base')

    op.alter_column(
        'plugin_info',
        'operator_schema',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'assets',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'demo_config',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'plugin_info',
        'sources',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'computation',
        'artifact_errors',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=False,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'attachments',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )
    op.alter_column(
        'artifact',
        'sources',
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        type_=postgresql.JSON(astext_type=sa.Text()),
        existing_nullable=True,
        schema='ca_base',
    )

    op.drop_constraint('artifact_correlation_uuid_fkey', 'artifact', schema='ca_base')
    op.drop_constraint('computation_lookup_computation_id_fkey', 'computation_lookup', schema='ca_base')
    op.alter_column(
        'artifact',
        'correlation_uuid',
        existing_type=sa.String(),
        type_=sa.UUID(),
        existing_nullable=False,
        schema='ca_base',
        postgresql_using='correlation_uuid::uuid',
    )
    op.alter_column(
        'computation',
        'correlation_uuid',
        existing_type=sa.String(),
        type_=sa.UUID(),
        existing_nullable=False,
        schema='ca_base',
        postgresql_using='correlation_uuid::uuid',
    )
    op.alter_column(
        'computation_lookup',
        'computation_id',
        existing_type=sa.String(),
        type_=sa.UUID(),
        existing_nullable=False,
        schema='ca_base',
        postgresql_using='computation_id::uuid',
    )
    op.create_foreign_key(
        None,
        'artifact',
        'computation',
        ['correlation_uuid'],
        ['correlation_uuid'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )
    op.create_foreign_key(
        None,
        'computation_lookup',
        'computation',
        ['computation_id'],
        ['correlation_uuid'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )
