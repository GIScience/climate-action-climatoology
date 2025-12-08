import json
import re
import tomllib
from datetime import timedelta
from enum import StrEnum
from functools import cached_property
from io import BytesIO
from pathlib import Path
from typing import Annotated, Any, List, Literal, Optional, Set, Union

import bibtexparser
import geojson_pydantic
from PIL import Image
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    HttpUrl,
    ValidationError,
    computed_field,
    conlist,
    model_validator,
)
from pydantic.json_schema import JsonSchemaValue
from semver import Version

from climatoology.base import PydanticSemver
from climatoology.base.logging import get_climatoology_logger

log = get_climatoology_logger(__name__)

DEMO_AOI_PATH = Path(__file__).parent.parent / 'resources/Heidelberg_AOI.geojson'


class Concern(StrEnum):
    """Keywords that group plugins by topic."""

    CLIMATE_ACTION__GHG_EMISSION = 'ghg_emission'
    CLIMATE_ACTION__MITIGATION = 'mitigation'
    CLIMATE_ACTION__ADAPTION = 'adaption'

    MOBILITY_PEDESTRIAN = 'pedestrian'
    MOBILITY_CYCLING = 'cycling'

    SUSTAINABILITY__WASTE = 'waste'


class PluginState(StrEnum):
    EXPERIMENTAL = 'experimental'
    ACTIVE = 'active'
    HIBERNATE = 'hibernate'
    ARCHIVE = 'archive'


class PluginAuthor(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str = Field(description='The author name.', examples=['John Doe'])
    affiliation: Optional[str] = Field(
        description="The author's affiliation statement. Leave blank if you are a HeiGIT member.",
        examples=['HeiGIT gGmbH'],
        default=None,
    )
    website: Optional[HttpUrl] = Field(
        description="The name will be linked to this website. HeiGIT members who don't "
        'have a dedicated may link to the general team website '
        '(see example).',
        examples=['https://heigit.org/heigit-team/'],
        default=None,
    )


class BaseSource(BaseModel):
    ID: str
    title: str
    author: str
    year: str
    note: Optional[str] = None


class ArticleSource(BaseSource):
    ENTRYTYPE: Literal['article']
    journal: str
    volume: str
    number: Optional[str] = None
    pages: str
    url: Optional[str] = None


class IncollectionSource(BaseSource):
    ENTRYTYPE: Literal['inbook', 'inproceedings']
    booktitle: str
    pages: str
    url: Optional[str] = None


class MiscSource(BaseSource):
    ENTRYTYPE: Literal['misc']
    url: str


type Source = Union[ArticleSource, IncollectionSource, MiscSource]


class Assets(BaseModel):
    """Static data linked to the plugin that may be used by the operator."""

    icon: FilePath = Field(description='The icon file', examples=[Path('icon.png')])
    sources_library: dict[str, Source] = Field(
        description='The sources available in this plugin',
        examples=[
            {
                'id1': MiscSource(
                    ENTRYTYPE='misc',
                    ID='id1',
                    title='title1',
                    author='author1',
                    year='2008',
                    url='https://example.com',
                )
            }
        ],
        default=dict(),
    )


class AssetsFinal(BaseModel):
    """Static data linked to the plugin that should be stored in the object store."""

    icon: str = Field(description='The icon asset', examples=['icon.png'])


class CustomAOI(BaseModel):
    """Configuration to define a custom AOI from a spatial file."""

    name: str = Field(description='The display name of the custom AOI', examples=['Earth'])
    path: Path = Field(
        description='The path to the file containing the custom AOI', examples=['/path/to/earth.geojson']
    )

    @computed_field
    @property
    def geojson(self) -> geojson_pydantic.MultiPolygon:
        return geojson_pydantic.MultiPolygon(**json.loads(self.path.read_text()))


class DemoConfig(BaseModel):
    """Configuration to run a demonstration of a plugin."""

    params: dict[str, Any] = Field(description='The input parameters used for the demo.')
    name: str = Field(description='The display name of the demo AOI')
    aoi: geojson_pydantic.MultiPolygon = Field(
        description='The the area of interest the demo will be run in.',
        examples=[
            geojson_pydantic.MultiPolygon(
                **{
                    'type': 'MultiPolygon',
                    'coordinates': [
                        [
                            [
                                [0.0, 0.0],
                                [0.0, 1.0],
                                [1.0, 1.0],
                                [0.0, 0.0],
                            ]
                        ]
                    ],
                }
            )
        ],
    )


class _PluginBaseInfo(BaseModel):
    """A dataclass containing the consistent attributes between the PluginInfo and PluginInfoEnriched"""

    name: str = Field(description='The full name of the plugin.', examples=['The Plugin'])
    authors: Annotated[
        conlist(item_type=PluginAuthor, min_length=1),
        Field(description='A list of plugin contributors.', examples=[[PluginAuthor(name='John Doe')]]),
    ]
    state: PluginState = Field(
        description='The current development state of the plugin using categories from https://github.com/GIScience/badges.',
        examples=[PluginState.ACTIVE],
        default=PluginState.ACTIVE,
    )
    concerns: Set[Concern] = Field(
        description='The domains or topics the plugin is tackling. It can be used as a set of keywords that '
        'group multiple plugins.',
        examples=[{Concern.CLIMATE_ACTION__GHG_EMISSION, Concern.CLIMATE_ACTION__MITIGATION}],
    )
    teaser: str = Field(
        description='A single sentence teaser for the plugins functionality. The sentence must be between 20 and 150 '
        'characters long, start with an upper case letter and end with a full stop.',
        examples=['Calculate your path to become CO2 neutral by 2030.'],
        min_length=20,
        max_length=150,
        pattern='^[A-Z].*\\.$',
    )
    computation_shelf_life: Optional[timedelta] = Field(
        description='How long are computations valid (at most). Computations will be valid within a fixed time frame '
        'of `shelf_life`. The fix timeframe starts at UNIX TS 0 and renews every `shelf_life`. A time delta of 0 means '
        'no caching while None means indefinite caching.',
        examples=[timedelta(weeks=4)],
        default=timedelta(0),
    )


class PluginInfo(_PluginBaseInfo):
    """A dataclass to provide the basic information about a plugin."""

    purpose: FilePath = Field(
        description='A markdown file that describes: What will this plugin accomplish?',
        examples=['purpose.md'],
    )
    methodology: FilePath = Field(
        description='A markdown file that explains: How does this plugin achieve its goal?',
        examples=['methodology.md'],
    )
    icon: FilePath = Field(description='The path to the icon image.', examples=[Path('icon.png')])
    sources_library: Optional[FilePath] = Field(
        description='The path to the sources library. Provide a [.bib](https://bibtex.eu/faq/how-do-i-create-a-bib-file-to-manage-my-bibtex-references/)'
        'file. You can extract such a file from most common bibliography management systems.',
        examples=[Path('sources_library.json')],
        default=None,
    )
    info_source_keys: Optional[set[str]] = Field(
        description='A list of keys/IDs to optionally subset the sources library to only include the base sources for '
        'the plugin. Defaults to all sources.',
        examples=['source_1'],
        default=None,
    )
    demo_input_parameters: BaseModel = Field(description='The input parameters used for the demo.')
    demo_aoi: CustomAOI = Field(
        description='The AOI to use for demo computations',
        default=CustomAOI(name='Heidelberg Demo', path=DEMO_AOI_PATH),
    )

    @computed_field
    @property
    def id(self) -> str:
        plugin_id = self.name.lower()
        plugin_id = re.sub(r'[^a-zA-Z-\s]', '', plugin_id)
        plugin_id = re.sub(r'\s', '_', plugin_id)
        return plugin_id

    @computed_field
    @cached_property
    def version(self) -> PydanticSemver:
        version = extract_attribute_from_pyproject_toml(attribute_tree=['project', 'version'])
        try:
            return PydanticSemver.parse(version)
        except (TypeError, ValueError) as e:
            raise ValueError(
                'Your pyproject.toml does not contain a version or is not adhering to the latest pyproject.toml '
                'format: https://python-poetry.org/docs/pyproject/'
            ) from e

    @computed_field
    @cached_property
    def repository(self) -> HttpUrl:
        repository = extract_attribute_from_pyproject_toml(attribute_tree=['project', 'urls', 'repository'])
        try:
            return HttpUrl(repository)
        except ValidationError as e:
            raise ValueError(
                'Your pyproject.toml does not contain a repository url or is not adhering to the latest pyproject.toml '
                'format: https://python-poetry.org/docs/pyproject/'
            ) from e

    @computed_field
    @cached_property
    def assets(self) -> Assets:
        # The conversion and validation from a dict to the Source type will happen on Asset instantiation
        # noinspection PyTypeChecker
        sources_library: dict[str, Source] = _convert_bib(self.sources_library)
        assets = Assets(sources_library=sources_library, icon=self.icon)
        return assets

    @computed_field
    @property
    def sources(self) -> list[Source]:
        return filter_sources(sources_library=self.assets.sources_library, source_keys=self.info_source_keys)

    @computed_field
    @property
    def demo_params_as_dict(self) -> dict[str, Any]:
        return self.demo_input_parameters.model_dump(mode='json')

    @model_validator(mode='after')
    def validate(self):
        """This validator asserts that validations happen early"""
        assert self.version, "there is an issue with the plugin's version number"
        assert self.repository, 'the repository URL could not be found'
        assert self.demo_aoi.geojson, "the geojson for the demo AOI couldn't be loaded"
        assert self.assets, "assets weren't created correctly"
        assert isinstance(self.sources, list), 'there was a problem generating the source list for the plugin'
        assert isinstance(self.demo_params_as_dict, dict), "the demo input parameters couldn't be loaded"


class PluginInfoEnriched(_PluginBaseInfo):
    model_config = ConfigDict(from_attributes=True)

    id: str = Field(description='A cleaned plugin name.', examples=['the_plugin_001'])
    version: Annotated[
        Version,
        PydanticSemver,
        Field(
            description='The plugin version.',
            examples=[str(Version(0, 0, 1))],  # https://github.com/pydantic/pydantic/issues/12280
        ),
    ]
    repository: HttpUrl = Field(
        description='The link to the source code of the plugin.',
        examples=[HttpUrl('https://gitlab.heigit.org/climate-action/net_zero')],
    )
    purpose: str = Field(
        description='What will this plugin accomplish?',
        examples=['This plugin provides information on a special aspect of climate action.'],
    )
    methodology: str = Field(
        description='How does this plugin achieve its goal?',
        examples=['This plugin uses a combination of data source A and method B to accomplish the purpose.'],
    )
    sources: list[Source] = Field(
        description='A list of sources that were used in the process or are related. ',
        examples=[
            [
                MiscSource(
                    ENTRYTYPE='misc',
                    ID='id1',
                    title='title1',
                    author='author1',
                    year='2008',
                    url='https://example.com',
                )
            ]
        ],
    )
    assets: Assets = Field(description='Static assets')
    operator_schema: JsonSchemaValue = Field(
        description='The schematic description of the parameters necessary to initiate a computation using '
        '[JSONSchema](https://json-schema.org/).',
        examples=[
            {
                'properties': {
                    'bool': {
                        'description': 'A required boolean parameter.',
                        'examples': [True],
                        'title': 'Boolean Input',
                        'type': 'boolean',
                    },
                    'required': [
                        'bool',
                    ],
                    'title': 'ComputeInput',
                    'type': 'object',
                }
            }
        ],
    )
    demo_config: DemoConfig = Field(
        description='The configuration for the demo computation',
        examples=[
            DemoConfig(
                name='Heidelberg Demo',
                params={},
                aoi=geojson_pydantic.MultiPolygon.create(
                    coordinates=[
                        [
                            [
                                [0.0, 0.0],
                                [0.0, 1.0],
                                [1.0, 1.0],
                                [0.0, 0.0],
                            ]
                        ]
                    ]
                ),
            )
        ],
    )
    library_version: Annotated[
        Version,
        PydanticSemver,
        Field(
            description='The climatoology library version, the plugin is using.',
            examples=[str(Version(1, 2, 3))],  # https://github.com/pydantic/pydantic/issues/12280
        ),
    ]


class PluginInfoFinal(PluginInfoEnriched):
    assets: AssetsFinal = Field(description='Static assets', examples=[AssetsFinal(icon='icon.png')])


def extract_attribute_from_pyproject_toml(attribute_tree: list[str]) -> Optional[str]:
    with open('pyproject.toml', 'rb') as pyproject_toml:
        pyproject = tomllib.load(pyproject_toml)
        curr_subtree = pyproject
        for key in attribute_tree:
            curr_subtree = curr_subtree.get(key)
            if curr_subtree is None:
                return None
        return curr_subtree


def _verify_icon(icon: Path) -> None:
    image = Image.open(icon)
    image.verify()


def _convert_icon_to_thumbnail(icon: Path) -> BytesIO:
    _verify_icon(icon)
    image = Image.open(icon)
    image.thumbnail((500, 500))
    buffered = BytesIO()
    image.save(buffered, format='PNG')
    buffered.seek(0)
    return buffered


def _convert_bib(sources: Optional[Path] = None) -> dict[str, dict[str, str]]:
    if sources is None:
        return dict()
    with open(sources, mode='r') as file:
        return bibtexparser.load(file).get_entry_dict()


def filter_sources(sources_library: dict[str, Source], source_keys: Optional[set[str]]) -> list[Source]:
    if source_keys is None:
        return list(sources_library.values())
    sources_filtered = []
    for key in source_keys:
        if key in sources_library:
            sources_filtered.append(sources_library[key])
        else:
            raise ValueError(
                f'The sources library does not contain a source with the id: {key}. '
                'Check the keys in your sources bib file provided to the generate_plugin_info '
                'method.'
            )
    return sources_filtered


def generate_plugin_info(
    *,
    name: str,
    authors: List[PluginAuthor],
    concerns: Set[Concern],
    teaser: str,
    purpose: Path,
    methodology: Path,
    icon: Path,
    demo_input_parameters: BaseModel,
    demo_aoi: CustomAOI = CustomAOI(name='Heidelberg Demo', path=DEMO_AOI_PATH),
    state: PluginState = PluginState.ACTIVE,
    computation_shelf_life: timedelta = timedelta(0),
    sources_library: Optional[Path] = None,
    info_source_keys: Optional[set[str]] = None,
) -> PluginInfo:
    """Generate a plugin info object.

    :param name: The full name of the plugin. Try to make it concise.
    :param authors: The list of plugin contributors. The list should be limited to contributors that have invested a
      considerable amount of contributions to the plugin. The list should adhere to the research-paper order i.e. by
      amount of contributions, descending.
    :param concerns: The domains or topics the plugin is tackling.
    :param teaser: A single sentence teaser for the plugins' functionality. The sentence must be between 20 and 150
      characters long, start with an upper case letter and end with a full stop.
    :param purpose: What will this plugin accomplish? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param methodology: How does this plugin achieve its goal? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param icon: The path to an image or icon that can be used to represent the plugin. Make sure the file is committed
      to the repository and HeiGIT has all legal rights to it (without attribution!).
    :param demo_input_parameters: the input parameters for the plugin.
    :param demo_aoi: A `CustomAOI` object defining the AOI name and file to use for the demo computation.
    :param state: The current development state of the plugin using categories from https://github.com/GIScience/badges.
    :param computation_shelf_life: How long are computations valid (at most). Computations will be valid within a fixed
      time frame of `shelf_life`. The fix timeframe starts at UNIX TS 0 and renews every `shelf_life`. A time delta of
      0 means no caching while None means indefinite caching.
    :param sources_library: A list of sources that were used in the process or are related. Self-citations are welcome and
      even preferred! Provide a [.bib](https://bibtex.eu/faq/how-do-i-create-a-bib-file-to-manage-my-bibtex-references/)
      file. You can extract such a file from most common bibliography management systems.
    :param info_source_keys: A list of IDs to optionally subset the sources library to only include the base sources for the
      plugin. Defaults to all sources.
    :return: A PluginInfo object that can be used to announce the plugin on the platform.
    """

    return PluginInfo(
        name=name,
        authors=authors,
        state=state,
        concerns=concerns,
        teaser=teaser,
        computation_shelf_life=computation_shelf_life,
        purpose=purpose,
        methodology=methodology,
        icon=icon,
        sources_library=sources_library,
        info_source_keys=info_source_keys,
        demo_input_parameters=demo_input_parameters,
        demo_aoi=demo_aoi,
    )
