"""add planned state

Revision ID: 8f6ee7328f1e
Revises: f398f777a563
Create Date: 2026-06-03 17:44:02.385103

"""

from typing import Sequence, Union

from alembic import op
from alembic_postgresql_enum import TableReference

# revision identifiers, used by Alembic.
revision: str = '8f6ee7328f1e'
down_revision: Union[str, None] = 'f398f777a563'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.sync_enum_values(
        enum_schema='public',
        enum_name='pluginstate',
        new_values=['PLANNED', 'EXPERIMENTAL', 'ACTIVE', 'HIBERNATE', 'ARCHIVE'],
        affected_columns=[TableReference(table_schema='ca_base', table_name='plugin_info', column_name='state')],
        enum_values_to_rename=[],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.sync_enum_values(
        enum_schema='public',
        enum_name='pluginstate',
        new_values=['EXPERIMENTAL', 'ACTIVE', 'HIBERNATE', 'ARCHIVE'],
        affected_columns=[TableReference(table_schema='ca_base', table_name='plugin_info', column_name='state')],
        enum_values_to_rename=[],
    )
