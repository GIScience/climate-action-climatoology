import inspect
from pathlib import Path

from climatoology.store.database import migration


def discover() -> Path:
    ini_file_path = Path(inspect.getfile(migration)).parent / 'alembic.ini'
    print(ini_file_path)
    return ini_file_path
