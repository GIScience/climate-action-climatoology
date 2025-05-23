import datetime
from typing import Optional, List, Set
from uuid import UUID

import sqlalchemy
from geoalchemy2 import Geometry, WKTElement
from pydantic.json_schema import JsonSchemaValue
from semver import Version

from sqlalchemy import Table, Column, ForeignKey, String, JSON, MetaData, UniqueConstraint, Computed
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from climatoology.base.artifact import ArtifactModality, Attachments
from climatoology.base.event import ComputationState
from climatoology.base.info import Concern, DemoConfig, Assets, PluginState

SCHEMA_NAME = 'ca_base'
COMPUTATION_DEDUPLICATION_CONSTRAINT = 'computation_deduplication_constraint'


class Base(DeclarativeBase):
    metadata = MetaData(schema=SCHEMA_NAME)


author_info_link_table = Table(
    'author_info_link_table',
    Base.metadata,
    Column('info_id', ForeignKey('info.plugin_id')),
    Column('author_id', ForeignKey('pluginauthor.name')),
)


class PluginAuthorTable(Base):
    __tablename__ = 'pluginauthor'

    name: Mapped[str] = mapped_column(primary_key=True)
    affiliation: Mapped[Optional[str]]
    website: Mapped[Optional[str]]


class InfoTable(Base):
    __tablename__ = 'info'

    plugin_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    authors: Mapped[List[PluginAuthorTable]] = relationship(secondary=author_info_link_table)
    version: Mapped[Version] = mapped_column(String)
    concerns: Mapped[Set[Concern]] = mapped_column(ARRAY(sqlalchemy.Enum(Concern)))
    state: Mapped[PluginState] = mapped_column(sqlalchemy.Enum(PluginState))
    teaser: Mapped[Optional[str]]
    purpose: Mapped[str]
    methodology: Mapped[str]
    sources: Mapped[Optional[List[dict]]] = mapped_column(JSON)
    demo_config: Mapped[Optional[DemoConfig]] = mapped_column(JSON)
    computation_shelf_life: Mapped[Optional[datetime.timedelta]]
    assets: Mapped[Assets] = mapped_column(JSON)
    operator_schema: Mapped[JsonSchemaValue] = mapped_column(JSON)
    library_version: Mapped[Version] = mapped_column(String)


class ArtifactTable(Base):
    __tablename__ = 'artifact'

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    correlation_uuid: Mapped[UUID] = mapped_column(ForeignKey('computation.correlation_uuid'))
    name: Mapped[str]
    modality: Mapped[ArtifactModality]
    primary: Mapped[bool]
    tags: Mapped[Optional[Set[str]]] = mapped_column(ARRAY(sqlalchemy.String))
    summary: Mapped[str]
    description: Mapped[Optional[str]]
    attachments: Mapped[Optional[Attachments]] = mapped_column(JSON)
    store_id: Mapped[str]
    file_path: Mapped[str]


class ComputationTable(Base):
    __tablename__ = 'computation'
    __table_args__ = (
        UniqueConstraint(
            'deduplication_key',  # using an md5 hash creates the possibility for cash collisions but raw columns will exceed the cache entry size
            'cache_epoch',
            name=COMPUTATION_DEDUPLICATION_CONSTRAINT,
        ),
    )

    correlation_uuid: Mapped[UUID] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime]
    deduplication_key: Mapped[UUID] = mapped_column(
        Computed('md5(requested_params::text||st_astext(aoi_geom)||plugin_id||plugin_version)::uuid')
    )
    cache_epoch: Mapped[Optional[int]]
    valid_until: Mapped[datetime.datetime]
    params: Mapped[Optional[dict]] = mapped_column(JSONB)
    requested_params: Mapped[dict] = mapped_column(JSONB)
    aoi_geom: Mapped[WKTElement] = mapped_column(Geometry('MultiPolygon', srid=4326))
    artifacts: Mapped[List[ArtifactTable]] = relationship()
    plugin_id: Mapped[str] = mapped_column(ForeignKey('info.plugin_id'))
    plugin_version: Mapped[Version] = mapped_column(String)
    plugin: Mapped[InfoTable] = relationship()
    status: Mapped[ComputationState]
    message: Mapped[Optional[str]]
    artifact_errors: Mapped[dict[str, str]] = mapped_column(JSON)


class ComputationLookup(Base):
    __tablename__ = 'computation_lookup'

    user_correlation_uuid: Mapped[UUID] = mapped_column(primary_key=True)
    request_ts: Mapped[datetime.datetime]
    aoi_name: Mapped[str]
    aoi_id: Mapped[str]
    computation_id: Mapped[UUID] = mapped_column(ForeignKey('computation.correlation_uuid'))
    computation: Mapped[ComputationTable] = relationship()
