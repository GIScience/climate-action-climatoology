from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import PickleType, Sequence, Text
from sqlalchemy.orm import Mapped, mapped_column

from climatoology.base.artifact import _Artifact
from climatoology.store.database.models.base import PUBLIC_SCHEMA_NAME, ClimatoologyTableBase


class CeleryTaskMeta(ClimatoologyTableBase):
    __table_args__ = {'schema': PUBLIC_SCHEMA_NAME}
    __tablename__ = 'celery_taskmeta'

    id: Mapped[int] = mapped_column(Sequence('task_id_sequence'), primary_key=True, autoincrement=True)
    task_id: Mapped[Optional[str]] = mapped_column(unique=True)
    status: Mapped[Optional[str]] = mapped_column(default='PENDING')
    result: Mapped[Optional[list[_Artifact] | Exception]] = mapped_column(PickleType)
    date_done: Mapped[Optional[datetime]] = mapped_column(
        default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc)
    )
    traceback: Mapped[Optional[str]] = mapped_column(Text)
    name: Mapped[Optional[str]]
    args: Mapped[Optional[bytes]]
    kwargs: Mapped[Optional[bytes]]
    worker: Mapped[Optional[str]]
    retries: Mapped[Optional[int]]
    queue: Mapped[Optional[str]]


class CeleryTaskSetMeta(ClimatoologyTableBase):
    __table_args__ = {'schema': PUBLIC_SCHEMA_NAME}
    __tablename__ = 'celery_tasksetmeta'

    id: Mapped[int] = mapped_column(Sequence('task_id_sequence'), primary_key=True, autoincrement=True)
    taskset_id: Mapped[Optional[str]] = mapped_column(unique=True)
    result: Mapped[Optional[Any]] = mapped_column(PickleType)
    date_done: Mapped[Optional[datetime]] = mapped_column(default=datetime.now(timezone.utc))
