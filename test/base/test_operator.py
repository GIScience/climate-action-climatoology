import uuid

import pytest
from pydantic import ValidationError

from climatoology.base.computation import ComputationScope


def test_operator_info(default_info):
    assert default_info.version == '3.1.0'
    assert default_info.sources[0]['ENTRYTYPE'] == 'article'
    assert default_info.icon == ('data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDB'
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


def test_operator_typing(default_operator, default_computation_resources):
    default_operator.compute_unsafe(default_computation_resources, {'id': 1234, 'name': 'test'})

    with pytest.raises(ValidationError):
        default_operator.compute_unsafe(default_computation_resources, {'id': 'ID:1234', 'name': 'test'})


def test_operator_scope():
    correlation_uuid = uuid.uuid4()
    with ComputationScope(correlation_uuid) as resources:
        assert resources.correlation_uuid == correlation_uuid
        assert resources.computation_dir.exists()

    assert not resources.computation_dir.exists()
