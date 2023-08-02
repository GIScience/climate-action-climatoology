from dataclasses import dataclass
from enum import Enum
from uuid import UUID

import marshmallow_dataclass


@dataclass
class InfoCommand:
    correlation_uuid: UUID


class ReportCommandStatus(Enum):
    IN_PROGRESS = 1
    COMPLETED = 2
    FAILED = 3


@dataclass
class ReportCommandResult:
    correlation_uuid: UUID
    status: ReportCommandStatus


@dataclass
class ReportCommand:
    correlation_uuid: UUID
    params: dict


info_command_schema = marshmallow_dataclass.class_schema(InfoCommand)()
report_command_result_schema = marshmallow_dataclass.class_schema(ReportCommandResult)()
report_command_schema = marshmallow_dataclass.class_schema(ReportCommand)()
