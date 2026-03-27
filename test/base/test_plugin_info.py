from datetime import timedelta
from pathlib import Path

import pydantic
import pytest
from pydantic import HttpUrl, ValidationError

from climatoology.base.plugin_info import (
    Concern,
    IncollectionSource,
    MiscSource,
    PluginAuthor,
    PluginInfo,
    PluginInfoFinal,
    PluginState,
    extract_info_localisations,
    filter_sources,
    generate_plugin_info,
)


def test_generate_plugin_info_function(default_plugin_info: PluginInfo):
    generated_info = generate_plugin_info(
        name=default_plugin_info.name,
        authors=default_plugin_info.authors,
        concerns=default_plugin_info.concerns,
        localisation_directory=Path(__file__).parent.parent / 'resources/locales',
        icon=default_plugin_info.icon,
        demo_input_parameters=default_plugin_info.demo_input_parameters,
        state=default_plugin_info.state,
        computation_shelf_life=default_plugin_info.computation_shelf_life,
        sources_library=default_plugin_info.sources_library,
        info_source_keys=default_plugin_info.info_source_keys,
        demo_aoi=default_plugin_info.demo_aoi,
    )

    assert generated_info == default_plugin_info


def test_generate_plugin_info_function_no_localisation(default_plugin_info: PluginInfo, default_input_model):
    """Testing the deprecated functionality of providing only one language."""
    expected_info = PluginInfo(
        name='Test Plugin',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': (Path(__file__).parent.parent / 'resources/locales/en/teaser.txt').read_text()},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        sources_library=Path(__file__).parent.parent / 'resources/test.bib',
        computation_shelf_life=timedelta(days=1),
        demo_input_parameters=default_input_model,
    )

    generated_info = generate_plugin_info(
        name=default_plugin_info.name,
        authors=default_plugin_info.authors,
        concerns=default_plugin_info.concerns,
        teaser=default_plugin_info.teaser['en'],
        purpose=default_plugin_info.purpose['en'],
        methodology=default_plugin_info.methodology['en'],
        icon=default_plugin_info.icon,
        demo_input_parameters=default_plugin_info.demo_input_parameters,
        state=default_plugin_info.state,
        computation_shelf_life=default_plugin_info.computation_shelf_life,
        sources_library=default_plugin_info.sources_library,
        info_source_keys=default_plugin_info.info_source_keys,
        demo_aoi=default_plugin_info.demo_aoi,
    )

    assert generated_info == expected_info


def test_info_computed_fields(default_plugin_info):
    assert default_plugin_info.id == 'test_plugin'

    assert default_plugin_info.assets.sources_library['CitekeyInbook'] == IncollectionSource(
        ENTRYTYPE='inbook',
        ID='CitekeyInbook',
        title='Photosynthesis',
        author='Lisa A. Urry and Michael L. Cain and Steven A. Wasserman and Peter V. Minorsky and Jane B. Reece',
        year='2016',
        booktitle='Campbell Biology',
        pages='187--221',
    )
    assert Path(default_plugin_info.assets.icon).is_file()


def test_info_serialisable(default_plugin_info_final):
    """This is a sub-test of the following test to be able to quickly see which part of the test failed."""
    assert default_plugin_info_final.model_dump(mode='json')


def test_info_deserialisable(default_plugin_info_final):
    """This test asserts that the plugin info can be sent as a json object via celery"""
    serialised_info = default_plugin_info_final.model_dump(mode='json')
    info = PluginInfoFinal(**serialised_info)
    assert info == default_plugin_info_final


def test_plugin_id_special_characters(default_plugin_info):
    pi = default_plugin_info.model_copy(deep=True)
    pi.name = 'Test Plugin With $pecial Charact3ers²: CO₂'
    assert pi.id == 'test_plugin_with_pecial_characters_co'


def test_filter_sources_subsetting(default_sources):
    filtered_sources = filter_sources(
        sources_library={
            'CitekeyMisc': default_sources[0],
            'other_source': MiscSource(
                ID='other_source',
                title="Pluto: The 'Other' Red Planet",
                author='{NASA}',
                year='2015',
                note='Accessed: 2018-12-06',
                ENTRYTYPE='misc',
                url='https://www.nasa.gov/nh/pluto-the-other-red-planet',
            ),
        },
        source_keys={'CitekeyMisc'},
    )
    assert filtered_sources == [default_sources[0]]


def test_sources_are_optional(default_input_model):
    info = PluginInfo(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': 'Test teaser that is meant to do nothing.'},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        demo_input_parameters=default_input_model,
    )
    assert info.assets.sources_library == dict()


def test_filter_sources_with_invalid_key():
    test_sources_library = {}
    invalid_source_key = {'test_source'}
    with pytest.raises(
        ValueError,
        match='The sources library does not contain a source with the id: test_source. '
        'Check the keys in your sources bib file provided to the generate_plugin_info '
        'method.',
    ):
        filter_sources(sources_library=test_sources_library, source_keys=invalid_source_key)


def test_invalid_sources(default_input_model):
    with pytest.raises(pydantic.ValidationError, match=r'.*ArticleSource\.pages.*'):
        PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'en': 'Test teaser that is meant to do nothing.'},
            purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
            sources_library=Path(__file__).parent.parent / 'resources/invalid_test.bib',
        )


def test_short_teaser(default_input_model):
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at least 20 characters'):
        PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'en': 'This.'},
            purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_long_teaser(default_input_model):
    with pytest.raises(expected_exception=ValidationError, match=r'String should have at most 150 characters'):
        PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={
                'en': 'This Lorem ipsum dolor sit amet, consectetur adipiscing elit. Proin non feugiat felis. In pretium malesuada nisl non gravida. Sed tincidunt felis quis ipsum convallis venenatis. Vivamus vitae pulvinar magna.'
            },
            purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_small_start_teaser(default_input_model):
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'en': 'this plugin does nothing.'},
            purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_no_fullstop_teaser(default_input_model):
    with pytest.raises(expected_exception=ValidationError, match=r'String should match pattern'):
        PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'en': 'This plugin does nothing'},
            purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_default_plugin_state(default_input_model):
    computed_info = PluginInfo(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': 'Test teaser that is meant to do nothing.'},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        demo_input_parameters=default_input_model,
    )
    assert computed_info.state == PluginState.ACTIVE


def test_plugin_state(default_input_model):
    computed_info = PluginInfo(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': 'Test teaser that is meant to do nothing.'},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        demo_input_parameters=default_input_model,
        state=PluginState.ARCHIVE,
    )
    assert computed_info.state == PluginState.ARCHIVE


def test_shelf_life(default_input_model):
    computed_info = PluginInfo(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': 'Test teaser that is meant to do nothing.'},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        demo_input_parameters=default_input_model,
        computation_shelf_life=timedelta(hours=1),
    )
    assert computed_info.computation_shelf_life == timedelta(hours=1)


def test_repository_url(default_input_model):
    computed_info = PluginInfo(
        name='Test Plugin',
        icon=Path(__file__).parent.parent / 'resources/test_icon.png',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser={'en': 'Test teaser that is meant to do nothing.'},
        purpose={'en': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
        methodology={'en': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
        demo_input_parameters=default_input_model,
    )
    assert str(computed_info.repository) == 'https://gitlab.heigit.org/climate-action/climatoology'


def test_english_enforced(default_input_model):
    with pytest.raises(expected_exception=ValidationError, match=r"English localisation required, only {'de'} given"):
        _ = PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'de': 'Test teaser that is meant to do nothing.'},
            purpose={'de': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'de': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_language_set_equal(default_input_model):
    with pytest.raises(
        expected_exception=ValidationError,
        match=r"Localisation-set between string fields not aligned: teaser: {'en'}, purpose: {'de'}, methodology: {'fr'}",
    ):
        _ = PluginInfo(
            name='Test Plugin',
            icon=Path(__file__).parent.parent / 'resources/test_icon.png',
            authors=[
                PluginAuthor(
                    name='John Doe',
                    affiliation='HeiGIT gGmbH',
                    website=HttpUrl('https://heigit.org/heigit-team/'),
                )
            ],
            concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
            teaser={'en': 'Test teaser that is meant to do nothing.'},
            purpose={'de': Path(__file__).parent.parent / 'resources/locales/en/purpose.md'},
            methodology={'fr': Path(__file__).parent.parent / 'resources/locales/en/methodology.md'},
            demo_input_parameters=default_input_model,
        )


def test_extract_info_localisations():
    localisations = extract_info_localisations(Path(__file__).parent.parent / 'resources/locales')

    for loc in localisations:
        assert set(loc.keys()) == {'en', 'de'}
