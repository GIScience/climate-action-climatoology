from datetime import datetime
from typing import Optional

from sqlalchemy import Text
from sqlalchemy.orm import Mapped, mapped_column

from climatoology.store.database.models.base import PUBLIC_SCHEMA_NAME, ClimatoologyTableBase


class CeleryTaskMeta(ClimatoologyTableBase):
    __table_args__ = {'schema': PUBLIC_SCHEMA_NAME}
    __tablename__ = 'celery_taskmeta'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    task_id: Mapped[Optional[str]] = mapped_column(unique=True)
    status: Mapped[Optional[str]]
    result: Mapped[Optional[bytes]]
    date_done: Mapped[Optional[datetime]]
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

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=False)
    taskset_id: Mapped[Optional[str]] = mapped_column(unique=True)
    result: Mapped[Optional[bytes]]
    date_done: Mapped[Optional[datetime]]
