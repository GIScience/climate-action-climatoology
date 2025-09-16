"""make_status_optional

Revision ID: d77cdd5e2fb2
Revises: b19b6b5c427f
Create Date: 2025-07-04 18:12:14.571923

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd77cdd5e2fb2'
down_revision: Union[str, None] = 'b19b6b5c427f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.alter_column('computation', 'status', nullable=True, schema='ca_base')


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column('computation', 'status', nullable=False, schema='ca_base')
