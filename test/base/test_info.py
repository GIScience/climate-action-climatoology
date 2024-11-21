from pathlib import Path

import pytest
from pydantic import ValidationError
from semver import Version

from climatoology.base.info import PluginAuthor, Concern, generate_plugin_info


def test_operator_info(default_info):
    assert default_info.version == '3.1.0'
    assert default_info.sources[0]['ENTRYTYPE'] == 'article'
    assert Path(default_info.assets.icon).is_file()


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
