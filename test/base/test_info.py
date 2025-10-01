import logging
from datetime import timedelta
from pathlib import Path

import geojson_pydantic
import pydantic
import pytest
from pydantic import HttpUrl, ValidationError
from semver import Version

from climatoology.base.info import (
    Concern,
    DemoConfig,
    PluginAuthor,
    PluginState,
    _Info,
    compose_demo_config,
    generate_plugin_info,
)
from test.conftest import TestModel

DEMO_AOI_PATH = Path(__file__).parent.parent / 'resources/test_aoi.geojson'


def test_demo_config_default_aoi():
    demo_config = compose_demo_config(input_parameters=TestModel(id=1))

    assert demo_config.name == 'Heidelberg Demo'
    assert isinstance(demo_config.aoi, geojson_pydantic.MultiPolygon)


def test_demo_config_custom_aoi_and_name(default_aoi_feature_geojson_pydantic):
    custom_aoi_name = 'custom_demo'
    demo_config = compose_demo_config(
        input_parameters=TestModel(id=1), aoi_name=custom_aoi_name, aoi_path=DEMO_AOI_PATH
    )

    assert demo_config.name == custom_aoi_name
    assert demo_config.aoi == default_aoi_feature_geojson_pydantic.geometry


def test_demo_config_custom_aoi_and_no_name(default_aoi_feature_geojson_pydantic):
    with pytest.raises(
        ValueError, match='You provided `aoi_path` but no `name`. Please include a `name` for the demo AOI'
    ):
        _ = compose_demo_config(input_parameters=TestModel(id=1), aoi_path=DEMO_AOI_PATH)


def test_demo_config_no_aoi_but_custom_name(caplog):
    expected_warning = 'You provided a `aoi_name` but not `aoi_path`. The default demo AOI and name will be used'
    with caplog.at_level(logging.WARNING):
        _ = compose_demo_config(input_parameters=TestModel(id=1), aoi_name='custom_demo')

    assert expected_warning in caplog.messages


def test_operator_info(default_info):
    assert default_info.id == 'test_plugin'
    assert default_info.version == Version(3, 1, 0)
    assert default_info.teaser == 'Test teaser that is meant to do nothing.'
    assert default_info.sources[0].ENTRYTYPE == 'article'
    assert Path(default_info.assets.icon).is_file()


def test_info_serialisable(default_info):
    assert default_info.model_dump(mode='json')


def test_info_deserialisable(default_info_final):
    serialised_info = default_info_final.model_dump(mode='json')
    info = _Info(**serialised_info)
    assert info == default_info_final


def test_plugin_id_special_characters():
    computed_info = generate_plugin_info(
        name='Test Plugin With $pecial Charact3ers²: CO₂',
        authors=[PluginAuthor(name='John Doe')],
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        sources=Path(__file__).parent.parent / 'resources/test.bib',
    )
    assert computed_info.id == 'test_plugin_with_pecial_characters_co'


def test_sources_are_optional(default_aoi_feature_geojson_pydantic):
    assert generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
    )


def test_invalid_sources(default_aoi_feature_geojson_pydantic):
    with pytest.raises(pydantic.ValidationError, match=r'.*ArticleSource\.pages.*'):
        generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            version=Version.parse('3.1.0'),
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser='Test teaser that is meant to do nothing.',
            purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
            methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
            sources=Path(__file__).parent.parent / 'resources/invalid_test.bib',
            demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        )


def test_demo_config_in_info(default_aoi_feature_geojson_pydantic):
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_config=compose_demo_config(
            input_parameters=TestModel(id=1),
            aoi_path=DEMO_AOI_PATH,
            aoi_name='demo',
        ),
    )
    assert isinstance(computed_info.demo_config, DemoConfig)


def test_short_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at least 20 characters'):
        generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
            demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        )


def test_long_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at most 150 characters'):
        generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
            demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        )


def test_small_start_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        _ = generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
            demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        )


def test_no_fullstop_teaser():
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        _ = generate_plugin_info(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
            demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        )


def test_default_plugin_state():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        version=Version.parse('3.1.0'),
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
    )
    assert computed_info.state == PluginState.ACTIVE


def test_plugin_state():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
        teaser='Test teaser that is meant to do nothing.',
        purpose=Path(__file__).parent.parent / 'resources/test_purpose.md',
        methodology=Path(__file__).parent.parent / 'resources/test_methodology.md',
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
    )
    assert computed_info.state == PluginState.ARCHIVE


def test_shelf_life():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        computation_shelf_life=timedelta(hours=1),
    )
    assert computed_info.computation_shelf_life == timedelta(hours=1)


def test_repository_url():
    computed_info = generate_plugin_info(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
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
        demo_config=compose_demo_config(input_parameters=TestModel(id=1)),
        computation_shelf_life=timedelta(hours=1),
    )
    assert str(computed_info.repository) == 'https://gitlab.heigit.org/climate-action/climatoology'
