"""create_playground_schema

Revision ID: 8945b7d5fac8
Revises: 44579dd466ad
Create Date: 2026-07-20 10:24:59.631228

"""

from typing import Sequence, Union

from alembic import op
from sqlalchemy.sql.ddl import CreateSchema, DropSchema

# revision identifiers, used by Alembic.
revision: str = '8945b7d5fac8'
down_revision: Union[str, None] = '44579dd466ad'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(CreateSchema('playground', if_not_exists=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(DropSchema('playground'))
