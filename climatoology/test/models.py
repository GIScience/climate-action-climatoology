from enum import StrEnum

from pydantic import BaseModel, Field

from climatoology.base.i18n import N_


class Option(StrEnum):
    OPT1 = N_('OPT1')
    OPT2 = N_('OPT2')


class Mapping(BaseModel):
    key: str = 'value'


class TestModel(BaseModel):
    __test__ = False
    id: int = Field(title=N_('ID'), description=N_('A required integer parameter.'), examples=[1])
    execution_time: float = Field(
        title=N_('Execution time'),
        description=N_('The time for the compute to run (in seconds)'),
        examples=[10.0],
        default=0.0,
    )
    name: str = Field(
        title=N_('Name'), description=N_('An optional name parameter.'), examples=['John Doe'], default='John Doe'
    )
    option: Option = Option.OPT1
    mapping: Mapping = Mapping()
