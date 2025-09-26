from typing import Any, Optional

from semver import Version
from sqlalchemy import Dialect, String, TypeDecorator
from sqlalchemy.sql.type_api import _T


class DbSemver(TypeDecorator):
    impl = String

    cache_ok = True

    def process_bind_param(self, value: Optional[_T], dialect: Dialect) -> Any:
        return str(value) if value else None

    def process_result_value(self, value: str, dialect: Dialect) -> Version:
        return Version.parse(value)
