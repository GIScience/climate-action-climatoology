import datetime
from pathlib import Path

import pytest
from pydantic import ValidationError, HttpUrl
from semver import Version

from climatoology.base.info import _Info, PluginAuthor, Concern, generate_plugin_info, DemoConfig, PluginState
from test.conftest import TestModel


def test_operator_info(default_info):
    assert default_info.version == '3.1.0'
    assert default_info.sources[0]['ENTRYTYPE'] == 'article'
    assert Path(default_info.assets.icon).is_file()


def test_info_serialisable(default_info):
    assert default_info.model_dump(mode='json')


def test_info_deserialisable(default_info_final):
    serialised_info = default_info_final.model_dump(mode='json')
    info = _Info(**serialised_info)
    assert info == default_info_final


def test_plugin_id():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
    assert computed_info.plugin_id == 'test_plugin'


def test_plugin_id_special_characters():
    computed_info = generate_plugin_info(
        name='Test Plugin With $pecial Charact3ers²: CO₂',
        authors=[PluginAuthor(name='John Doe')],
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        sources=Path(__file__).parent.parent / 'resources/test.bib',
    )
    assert computed_info.plugin_id == 'test_plugin_with_pecial_characters_co'


def test_sources_are_optional():
    assert generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_input_parameters=TestModel(id=1),
    )


def test_provide_demo_params_and_aoi(default_aoi_feature_geojson_pydantic):
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_input_parameters=TestModel(id=1),
        demo_aoi=Path(__file__).parent.parent / 'resources/test_aoi.geojson',
    )
    assert computed_info.demo_config == DemoConfig(
        params={'id': 1, 'name': 'John Doe', 'execution_time': 0.0}, aoi=default_aoi_feature_geojson_pydantic.geometry
    )


def test_provide_demo_params_and_no_aoi():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_input_parameters=TestModel(id=1),
    )
    assert isinstance(computed_info.demo_config, DemoConfig)


def test_provide_no_demo_params_or_aoi():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
    assert computed_info.demo_config is None


def test_teaser():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='This plugin does nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
    assert computed_info.teaser == 'This plugin does nothing.'


def test_short_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at least 20 characters'):
        generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser='This.',
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        )


def test_long_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at most 150 characters'):
        generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser='This Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin non feugiat felis. In pretium malesuada nisl non gravida. Sed tincidunt felis quis ipsum convallis venenatis. Vivamus vitae pulvinar magna.',
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        )


def test_small_start_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        _ = generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser='this plugin does nothing.',
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        )


def test_no_fullstop_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        _ = generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser='This plugin does nothing',
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        )


def test_default_plugin_state():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
    assert computed_info.state == PluginState.ACTIVE


def test_plugin_state():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        state=PluginState.ARCHIVE,
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
    )
    assert computed_info.state == PluginState.ARCHIVE


def test_shelf_life():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.jpeg',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='This plugin does nothing and that is good.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        computation_shelf_life=datetime.timedelta(hours=1),
    )
    assert computed_info.computation_shelf_life == datetime.timedelta(hours=1)
