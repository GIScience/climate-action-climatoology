import os
from pathlib import Path
from typing import List

import pytest
from pydantic import BaseModel, ValidationError
from semver import Version

from climatoology.base.operator import Info, Operator, Artifact, Concern


def test_operator_info():
    description = Info(
        name='test_plugin',
        icon=Path('resources/test_icon.jpeg'),
        version=Version.parse('3.1.0'),
        concerns=[Concern.GHG_EMISSION],
        purpose='The purpose of this base is to '
                'present basic library properties in '
                'terms of enforcing similar capabilities '
                'between Climate Action event components',
        methodology='This is a test base',
        sources_bib=Path(f'{os.path.dirname(__file__)}/test.bib')
    )

    assert description.version == '3.1.0'
    assert description.sources['test2023']['ENTRYTYPE'] == 'article'
    assert description.icon == ('data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDB'
                                 'kSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjI'
                                 'yMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAACAAIDASIAAhEB'
                                 'AxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRB'
                                 'RIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1'
                                 'hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytL'
                                 'T1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QA'
                                 'tREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYG'
                                 'RomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoq'
                                 'OkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxE'
                                 'APwDwa5ubi5u5p555ZZpHZ3kdyzMxOSST1JPeiiigD//Z')


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
