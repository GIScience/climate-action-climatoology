from typing import Optional, List, Set

import sqlalchemy
from pydantic.json_schema import JsonSchemaValue
from semver import Version

from sqlalchemy import Table, Column, ForeignKey, String, JSON
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from climatoology.base.info import Concern, DemoConfig, Assets


class Base(DeclarativeBase):
    pass


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
    purpose: Mapped[str]
    methodology: Mapped[str]
    sources: Mapped[Optional[List[dict]]] = mapped_column(JSON)
    demo_config: Mapped[Optional[DemoConfig]] = mapped_column(JSON)
    assets: Mapped[Assets] = mapped_column(JSON)
    operator_schema: Mapped[JsonSchemaValue] = mapped_column(JSON)
    library_version: Mapped[Version] = mapped_column(String)
