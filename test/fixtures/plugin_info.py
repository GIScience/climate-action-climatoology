from datetime import timedelta

import pytest
from pydantic import HttpUrl
from pydantic_extra_types.language_code import LanguageAlpha2
from semver import Version

from climatoology.base.i18n import N_, tr
from climatoology.base.plugin_info import (
    DEFAULT_LANGUAGE,
    AssetsFinal,
    Concern,
    MiscSource,
    PluginAuthor,
    PluginInfo,
    PluginInfoEnriched,
    PluginInfoFinal,
)
from test.conftest import TEST_RESOURCES_DIR, TestModel


@pytest.fixture
def default_input_model() -> TestModel:
    return TestModel(id=1)


@pytest.fixture
def default_sources() -> list[MiscSource]:
    return [
        MiscSource(
            ID='CitekeyMisc',
            title="Pluto: The 'Other' Red Planet",
            author='{NASA}',
            year='2015',
            note='Accessed: 2018-12-06',
            ENTRYTYPE='misc',
            url='https://www.nasa.gov/nh/pluto-the-other-red-planet',
        )
    ]


@pytest.fixture
def default_plugin_key() -> str:
    return 'test_plugin-3.1.0-en'


@pytest.fixture
def default_plugin_info(default_input_model) -> PluginInfo:
    info = PluginInfo(
        name='Test Plugin',
        authors=[
            PluginAuthor(
                name='John Doe',
                affiliation='HeiGIT gGmbH',
                website=HttpUrl('https://heigit.org/heigit-team/'),
            )
        ],
        icon=TEST_RESOURCES_DIR / 'test_icon.png',
        concerns={Concern.CLIMATE_ACTION__GHG_EMISSION},
        teaser=N_('Test teaser that is meant to do nothing.'),
        purpose={
            'en': TEST_RESOURCES_DIR / 'locales/en/purpose.md',
            'de': TEST_RESOURCES_DIR / 'locales/de/purpose.md',
        },
        methodology={
            'en': TEST_RESOURCES_DIR / 'locales/en/methodology.md',
            'de': TEST_RESOURCES_DIR / 'locales/de/methodology.md',
        },
        sources_library=TEST_RESOURCES_DIR / 'test.bib',
        localisation_directory=TEST_RESOURCES_DIR / 'locales',
        computation_shelf_life=timedelta(days=1),
        demo_input_parameters=default_input_model,
    )
    info.version = Version(3, 1, 0)
    return info


@pytest.fixture
def default_plugin_info_enriched(default_operator) -> PluginInfoEnriched:
    return default_operator.info_enriched


@pytest.fixture
def default_plugin_info_final(default_plugin_info_enriched) -> PluginInfoFinal:
    language = DEFAULT_LANGUAGE
    teaser = default_plugin_info_enriched.teaser
    purpose = default_plugin_info_enriched.purpose[language]
    methodology = default_plugin_info_enriched.methodology[language]
    assets = AssetsFinal(icon='assets/test_plugin/latest/ICON.png')
    default_info_final = PluginInfoFinal(
        **default_plugin_info_enriched.model_dump(exclude={'teaser', 'purpose', 'methodology', 'assets'}),
        language=language,
        teaser=teaser,
        purpose=purpose,
        methodology=methodology,
        assets=assets,
    )
    return default_info_final


@pytest.fixture
def default_plugin_info_final_de(default_plugin_info_enriched, set_to_german) -> PluginInfoFinal:
    language = LanguageAlpha2('de')
    teaser = tr(default_plugin_info_enriched.teaser)
    purpose = default_plugin_info_enriched.purpose[language]
    methodology = default_plugin_info_enriched.methodology[language]
    assets = AssetsFinal(icon='assets/test_plugin/latest/ICON.png')
    default_info_final = PluginInfoFinal(
        **default_plugin_info_enriched.model_dump(exclude={'teaser', 'purpose', 'methodology', 'assets'}),
        language=language,
        teaser=teaser,
        purpose=purpose,
        methodology=methodology,
        assets=assets,
    )
    return default_info_final
