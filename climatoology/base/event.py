from datetime import datetime
from enum import StrEnum
from typing import Optional
from uuid import UUID

from celery.states import FAILURE, PENDING, RETRY, REVOKED, STARTED, SUCCESS
from pydantic import BaseModel


# TODO: re-introduce dead pre-commit and remove the dead code here and also move the computation state to computation
class InfoCommand(BaseModel):
    """Info request command attributes."""

    correlation_uuid: UUID


class ComputationState(StrEnum):
    """Available stati of computations.

    Based on the Celery states (https://docs.celeryq.dev/en/stable/userguide/tasks.html#built-in-states)
    """

    PENDING = PENDING
    STARTED = STARTED
    SUCCESS = SUCCESS
    FAILURE = FAILURE
    RETRY = RETRY
    REVOKED = REVOKED


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
