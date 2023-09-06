import os
from pathlib import Path
from typing import List

import pytest
from pydantic import BaseModel, ValidationError
from semver import Version

from climatoology.base.operator import Info, Operator, Artifact


def test_operator_info():
    description = Info(
        name='test_plugin',
        version=Version.parse('3.1.0'),
        purpose='The purpose of this base is to '
                'present basic library properties in '
                'terms of enforcing similar capabilities '
                'between Climate Action event components',
        methodology='This is a test base',
        sources_bib=Path(f'{os.path.dirname(__file__)}/test.bib')
    )
    assert description.version == '3.1.0'
    assert description.sources['test2023']['ENTRYTYPE'] == 'article'


def test_operator_typing():
    class TestModel(BaseModel):
        id: int
        name: str

    class TestOperator(Operator[TestModel]):

        def info(self) -> Info:
            pass

        def report(self, params: TestModel) -> List[Artifact]:
            pass

    operator = TestOperator()
    operator.report_unsafe({'id': 1234, 'name': 'test'})

    with pytest.raises(ValidationError):
        operator.report_unsafe({'id': 'ID:1234', 'name': 'test'})
