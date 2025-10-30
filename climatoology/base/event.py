from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


# TODO: re-introduce dead pre-commit and remove the dead code here and also move the computation state to computation
class InfoCommand(BaseModel):
    """Info request command attributes."""

    correlation_uuid: UUID


class ComputationState(Enum):
    """Available stati of computations.

    Based on the Celery states (https://docs.celeryq.dev/en/stable/userguide/tasks.html#built-in-states) plus some
    custom states.
    """

    PENDING = 'PENDING'
    STARTED = 'STARTED'
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    RETRY = 'RETRY'
    REVOKED = 'REVOKED'


class ComputeCommandResult(BaseModel):
    """Attributes of compute command return messages."""

    correlation_uuid: UUID
    status: ComputationState
    message: Optional[str] = None
    timestamp: datetime


class ComputeCommand(BaseModel):
    """Attributes of compute-triggering messaged."""

    correlation_uuid: UUID
    params: dict
