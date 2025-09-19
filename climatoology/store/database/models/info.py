from datetime import timedelta
from typing import List, Optional, Set

import sqlalchemy
from pydantic.json_schema import JsonSchemaValue
from semver import Version
from sqlalchemy import JSON, Column, ForeignKey, Integer, String, Table, asc
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship

from climatoology.base.info import Assets, Concern, DemoConfig, PluginState
from climatoology.store.database.models.base import CLIMATOOLOGY_SCHEMA_NAME, ClimatoologyTableBase

author_info_link_table = Table(
    'author_info_link_table',
    ClimatoologyTableBase.metadata,
    Column('info_id', ForeignKey(f'{CLIMATOOLOGY_SCHEMA_NAME}.info.plugin_id')),
    Column('author_id', ForeignKey(f'{CLIMATOOLOGY_SCHEMA_NAME}.pluginauthor.name')),
    Column('author_seat', Integer),
    schema=CLIMATOOLOGY_SCHEMA_NAME,
)


class PluginAuthorTable(ClimatoologyTableBase):
    __tablename__ = 'pluginauthor'
    __table_args__ = {'schema': CLIMATOOLOGY_SCHEMA_NAME}

    name: Mapped[str] = mapped_column(primary_key=True)
    affiliation: Mapped[Optional[str]]
    website: Mapped[Optional[str]]


class InfoTable(ClimatoologyTableBase):
    __tablename__ = 'info'
    __table_args__ = {'schema': CLIMATOOLOGY_SCHEMA_NAME}

    plugin_id: Mapped[str] = mapped_column(primary_key=True)
    name: Mapped[str]
    repository: Mapped[str]
    authors: Mapped[List[PluginAuthorTable]] = relationship(
        secondary=author_info_link_table, order_by=asc(author_info_link_table.c.author_seat)
    )
    version: Mapped[Version] = mapped_column(String)
    concerns: Mapped[Set[Concern]] = mapped_column(ARRAY(sqlalchemy.Enum(Concern)))
    state: Mapped[PluginState] = mapped_column(sqlalchemy.Enum(PluginState))
    teaser: Mapped[Optional[str]]
    purpose: Mapped[str]
    methodology: Mapped[str]
    sources: Mapped[Optional[List[dict]]] = mapped_column(JSON)
    demo_config: Mapped[Optional[DemoConfig]] = mapped_column(JSON)
    computation_shelf_life: Mapped[Optional[timedelta]]
    assets: Mapped[Assets] = mapped_column(JSON)
    operator_schema: Mapped[JsonSchemaValue] = mapped_column(JSON)
    library_version: Mapped[Version] = mapped_column(String)
