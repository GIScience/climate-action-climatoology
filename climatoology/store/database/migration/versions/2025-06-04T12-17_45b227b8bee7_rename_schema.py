"""rename schema

Revision ID: 45b227b8bee7
Revises: 70fe35822ab9
Create Date: 2025-06-04 12:17:56.308177

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '45b227b8bee7'
down_revision: Union[str, None] = '70fe35822ab9'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text('alter schema "ca-base" rename to ca_base'))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text('alter schema ca_base rename to "ca-base"'))
