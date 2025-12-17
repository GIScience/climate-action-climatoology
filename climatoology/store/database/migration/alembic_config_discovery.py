import inspect
from pathlib import Path

from climatoology.store.database import migration


def discover():
    print(Path(inspect.getfile(migration)).parent / 'alembic.ini')
