"""rename_info

Revision ID: 2d37688e948a
Revises: afbe6fd67545
Create Date: 2025-11-28 15:55:34.670492

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '2d37688e948a'
down_revision: Union[str, None] = 'afbe6fd67545'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text('alter table ca_base.info rename to plugin_info'))
    op.execute(sa.text('alter table ca_base.pluginauthor rename to plugin_author'))
    op.execute(sa.text('alter table ca_base.author_info_link_table rename to plugin_info_author_link'))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text('alter table ca_base.plugin_info_author_link rename to author_info_link_table'))
    op.execute(sa.text('alter table ca_base.plugin_author rename to pluginauthor'))
    op.execute(sa.text('alter table ca_base.plugin_info rename to info'))
