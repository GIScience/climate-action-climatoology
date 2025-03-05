from typing import TypeVar

from pydantic import BaseModel

T_co = TypeVar('T_co', bound=BaseModel, covariant=True)
