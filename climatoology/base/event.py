from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class InfoCommand(BaseModel):
    """Info request command attributes."""

    correlation_uuid: UUID


class ComputeCommandStatus(Enum):
    """Available stati of computations."""

    SCHEDULED = 'scheduled'
    IN_PROGRESS = 'in-progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    FAILED__WRONG_INPUT = 'wrong-input'


class ComputeCommandResult(BaseModel):
    """Attributes of compute command return messages."""

    correlation_uuid: UUID
    status: ComputeCommandStatus
    message: Optional[str] = None
    timestamp: datetime


class ComputeCommand(BaseModel):
    """Attributes of compute-triggering messaged."""

    correlation_uuid: UUID
    params: dict
