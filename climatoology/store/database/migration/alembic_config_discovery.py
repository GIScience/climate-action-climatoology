import inspect
import sys
from pathlib import Path
from typing import Literal

from climatoology.store.database import migration


def discover() -> Path:
    ini_file_path = Path(inspect.getfile(migration)).parent / 'alembic.ini'
    return ini_file_path


def main() -> Literal[0]:
    print(discover())
    return 0


if __name__ == '__main__':
    sys.exit(main())
