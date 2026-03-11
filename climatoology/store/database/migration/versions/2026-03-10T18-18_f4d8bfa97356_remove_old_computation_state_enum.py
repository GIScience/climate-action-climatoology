"""remove-old-computation-state-enum

Revision ID: f4d8bfa97356
Revises: eec8b7cd6825
Create Date: 2026-03-10 18:18:20.744120

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'f4d8bfa97356'
down_revision: Union[str, None] = 'eec8b7cd6825'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    sa.Enum(name='computationstate').drop(op.get_bind())


def downgrade() -> None:
    """Downgrade schema."""
    sa.Enum('PENDING', 'STARTED', 'SUCCESS', 'FAILURE', 'RETRY', 'REVOKED', name='computationstate').create(
        op.get_bind()
    )
