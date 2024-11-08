from pathlib import Path

import pytest
from pydantic import ValidationError
from semver import Version

from climatoology.base.info import PluginAuthor, Concern, generate_plugin_info


def test_operator_info(default_info):
    assert default_info.version == '3.1.0'
    assert default_info.sources[0]['ENTRYTYPE'] == 'article'
    assert default_info.icon == (
        'data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDB'
        'kSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjI'
        'yMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAACAAIDASIAAhEB'
        'AxEB/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRB'
        'RIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1'
        'hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytL'
        'T1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QA'
        'tREAAgECBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYG'
        'RomJygpKjU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoq'
        'OkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxE'
        'APwDwa5ubi5u5p555ZZpHZ3kdyzMxOSST1JPeiiigD//Z'
    )


def test_info_name():
    with pytest.raises(ValidationError, match=r'Special characters and numbers are not allowed in the name.'):
        generate_plugin_info(
            name='Test Plugin With $pecial Charact3ers',
            icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
            authors=[PluginAuthor(name='John Doe')],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
            sources=Path(__file__).parent.parent / 'resources/test.bib',
        )

    info = generate_plugin_info(
        name='Test-Plugin with spaces',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[PluginAuthor(name='John Doe')],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent.parent / 'resources/test.bib',
    )
    assert info.plugin_id == 'test_plugin_with_spaces'


def test_info_serialisable(default_info):
    assert default_info.model_dump(mode='json')


def test_sources_are_optional(default_info):
    assert generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website='https://heigit.org/heigit-team/',
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
