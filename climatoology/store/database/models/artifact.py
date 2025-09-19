from typing import Optional, Set
from uuid import UUID

import sqlalchemy
from sqlalchemy import JSON, ForeignKey
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from climatoology.base.artifact import ArtifactModality, Attachments
from climatoology.store.database.models.base import CLIMATOOLOGY_SCHEMA_NAME, ClimatoologyTableBase


class ArtifactTable(ClimatoologyTableBase):
    __table_args__ = {'schema': CLIMATOOLOGY_SCHEMA_NAME}
    __tablename__ = 'artifact'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    rank: Mapped[Optional[int]]
    correlation_uuid: Mapped[UUID] = mapped_column(
        ForeignKey(f'{CLIMATOOLOGY_SCHEMA_NAME}.computation.correlation_uuid')
    )
    name: Mapped[str]
    modality: Mapped[ArtifactModality]
    primary: Mapped[bool]
    tags: Mapped[Optional[Set[str]]] = mapped_column(ARRAY(sqlalchemy.String))
    summary: Mapped[str]
    description: Mapped[Optional[str]]
    attachments: Mapped[Optional[Attachments]] = mapped_column(JSON)
    store_id: Mapped[str]
    file_path: Mapped[str]
