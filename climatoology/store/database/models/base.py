from typing import ClassVar

from sqlalchemy import Selectable, Table
from sqlalchemy.orm import DeclarativeBase

CLIMATOOLOGY_SCHEMA_NAME = 'ca_base'
PUBLIC_SCHEMA_NAME = 'public'


class ClimatoologyTableBase(DeclarativeBase):
    pass


class ClimatoologyViewBase(DeclarativeBase):
    select_statement: ClassVar[Selectable]
    __table__: ClassVar[Table]
    pass
