from datetime import datetime
from typing import List, Optional
from uuid import UUID

from geoalchemy2 import Geometry, WKTElement
from sqlalchemy import Computed, ForeignKey, UniqueConstraint, asc
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from climatoology.store.database.models import DbUuidAsString
from climatoology.store.database.models.artifact import ArtifactTable
from climatoology.store.database.models.base import CLIMATOOLOGY_SCHEMA_NAME, ClimatoologyTableBase
from climatoology.store.database.models.info import PluginInfoTable

COMPUTATION_DEDUPLICATION_CONSTRAINT = 'computation_deduplication_constraint'


class ComputationTable(ClimatoologyTableBase):
    __tablename__ = 'computation'
    __table_args__ = (
        UniqueConstraint(
            'plugin_key',
            'deduplication_key',  # using an md5 hash creates the possibility for cash collisions but raw columns will exceed the cache entry size
            'cache_epoch',
            name=COMPUTATION_DEDUPLICATION_CONSTRAINT,
        ),
        {'schema': CLIMATOOLOGY_SCHEMA_NAME},
    )

    correlation_uuid: Mapped[UUID] = mapped_column(DbUuidAsString, primary_key=True)
    deduplication_key: Mapped[UUID] = mapped_column(Computed('md5(requested_params::text||st_astext(aoi_geom))::uuid'))
    cache_epoch: Mapped[Optional[int]]
    valid_until: Mapped[datetime] = mapped_column(index=True)
    params: Mapped[Optional[dict]] = mapped_column(JSONB)
    requested_params: Mapped[dict] = mapped_column(JSONB)
    aoi_geom: Mapped[WKTElement] = mapped_column(Geometry('MultiPolygon', srid=4326))
    artifacts: Mapped[List[ArtifactTable]] = relationship(order_by=asc(ArtifactTable.rank))
    plugin_key: Mapped[str] = mapped_column(ForeignKey(f'{CLIMATOOLOGY_SCHEMA_NAME}.plugin_info.key'), index=True)
    plugin: Mapped[PluginInfoTable] = relationship()
    message: Mapped[Optional[str]]
    artifact_errors: Mapped[dict[str, str]] = mapped_column(JSONB)


class ComputationLookupTable(ClimatoologyTableBase):
    __tablename__ = 'computation_lookup'
    __table_args__ = {'schema': CLIMATOOLOGY_SCHEMA_NAME}

    user_correlation_uuid: Mapped[UUID] = mapped_column(primary_key=True)
    request_ts: Mapped[datetime]
    aoi_name: Mapped[str]
    aoi_id: Mapped[str] = mapped_column(index=True)
    aoi_properties: Mapped[Optional[dict]] = mapped_column(JSONB)
    computation_id: Mapped[UUID] = mapped_column(
        DbUuidAsString, ForeignKey(f'{CLIMATOOLOGY_SCHEMA_NAME}.computation.correlation_uuid')
    )
    computation: Mapped[ComputationTable] = relationship()
