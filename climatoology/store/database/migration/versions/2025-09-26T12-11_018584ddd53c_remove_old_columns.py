"""remove_old_columns

Revision ID: 018584ddd53c
Revises: b117fa8afa0a
Create Date: 2025-09-26 12:11:09.022489

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy import text

# revision identifiers, used by Alembic.
revision: str = '018584ddd53c'
down_revision: Union[str, None] = 'b117fa8afa0a'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint(
        op.f('author_info_link_table_info_id_fkey'), 'author_info_link_table', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint(op.f('computation_plugin_id_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint('info_pkey', 'info', schema='ca_base', type_='primary')

    op.add_column('info', sa.Column('id', sa.String(), nullable=True, primary_key=True), schema='ca_base')
    op.execute(text('update ca_base.info set id = plugin_id where TRUE'))
    op.alter_column('info', 'id', nullable=False, schema='ca_base')

    op.create_primary_key('info_pkey', 'info', ['id'], schema='ca_base')
    op.create_foreign_key(
        None, 'author_info_link_table', 'info', ['info_id'], ['id'], source_schema='ca_base', referent_schema='ca_base'
    )
    op.create_foreign_key(
        None, 'computation', 'info', ['plugin_id'], ['id'], source_schema='ca_base', referent_schema='ca_base'
    )

    op.drop_column('info', 'plugin_id', schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint(
        op.f('author_info_link_table_info_id_fkey'), 'author_info_link_table', schema='ca_base', type_='foreignkey'
    )
    op.drop_constraint(op.f('computation_plugin_id_fkey'), 'computation', schema='ca_base', type_='foreignkey')
    op.drop_constraint('info_pkey', 'info', schema='ca_base', type_='primary')

    op.add_column('info', sa.Column('plugin_id', sa.String(), nullable=True, primary_key=True), schema='ca_base')
    op.execute(text('update ca_base.info set plugin_id = id where TRUE'))
    op.alter_column('info', 'plugin_id', nullable=False, schema='ca_base')

    op.create_primary_key('info_pkey', 'info', ['plugin_id'], schema='ca_base')
    op.create_foreign_key(
        None,
        'author_info_link_table',
        'info',
        ['info_id'],
        ['plugin_id'],
        source_schema='ca_base',
        referent_schema='ca_base',
    )
    op.create_foreign_key(
        None, 'computation', 'info', ['plugin_id'], ['plugin_id'], source_schema='ca_base', referent_schema='ca_base'
    )

    op.drop_column('info', 'id', schema='ca_base')
