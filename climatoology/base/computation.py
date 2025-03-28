import shutil
import tempfile
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel


class ComputationResources(BaseModel):
    """A directory computation resource."""

    correlation_uuid: UUID
    computation_dir: Path
    artifact_errors: dict[str, str] = {}


class ComputationScope:
    """Context manager to isolate working environments for computations."""

    def __init__(self, correlation_uuid: UUID):
        self.resources = ComputationResources(
            computation_dir=Path(tempfile.mkdtemp(prefix=str(correlation_uuid))), correlation_uuid=correlation_uuid
        )

    def __enter__(self):
        return self.resources

    def __exit__(self, *args):
        shutil.rmtree(self.resources.computation_dir)
