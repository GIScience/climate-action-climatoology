"""update concerns

Revision ID: 73de3acb3ef9
Revises: 54bec88540ca
Create Date: 2026-10-30 14:37:29.509316

"""

import warnings
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '73de3acb3ef9'
down_revision: Union[str, None] = '54bec88540ca'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text("ALTER TYPE concern ADD VALUE 'MOBILITY';"))
    op.execute(sa.text("ALTER TYPE concern ADD VALUE 'EMISSIONS_AND_ENERGY';"))
    op.execute(sa.text("ALTER TYPE concern ADD VALUE 'LAND_AND_NATURE';"))


def downgrade() -> None:
    """Downgrade schema."""
    warnings.warn(
        f"Downgrading from {revision} is incomplete. This step should remove the concerns 'MOBILITY', "
        "'EMISSIONS_AND_ENERGY' and 'LAND_AND_NATURE' but doesn't."
        'See source code comment for details.'
    )
    # The reason is that we would
    # a) need to map all plugins that have the new values to old ones and
    # b) implement `sync_enum_values` so it works this the enum being used in an array (the way we do it) because
    # postgres does not support removing values from enums natively.
