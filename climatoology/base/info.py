import json
import re
import tomllib
from datetime import timedelta
from enum import StrEnum
from io import BytesIO
from pathlib import Path
from typing import Annotated, List, Literal, Optional, Set

import bibtexparser
import geojson_pydantic
from PIL import Image
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, conlist, model_validator
from pydantic.json_schema import JsonSchemaValue
from semver import Version

import climatoology
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


class Assets(BaseModel):
    """Static data linked to the plugin that should be stored in the object store."""

    icon: str = Field(description='The icon asset', examples=['icon.png'])


class DemoConfig(BaseModel):
    """Configuration to run a demonstration of a plugin."""

    params: dict = Field(description='The input parameters used for the demo.')
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


class PluginBaseInfo(BaseModel):
    id: Optional[str] = Field(
        description='A cleaned plugin name.',
        examples=['the_plugin_001'],
        default=None,
    )
    version: Annotated[
        Version,
        PydanticSemver,
        Field(
            description='The plugin version.',
            examples=[str(Version(0, 0, 1))],  # https://github.com/pydantic/pydantic/issues/12280
        ),
    ]


class _Info(PluginBaseInfo, extra='forbid'):
    """A dataclass to provide the basic information about a plugin."""

    model_config = ConfigDict(from_attributes=True)

    name: str = Field(description='The full name of the plugin.', examples=['The Plugin'])
    authors: conlist(item_type=PluginAuthor, min_length=1) = Field(
        description='A list of plugin contributors.', examples=[[PluginAuthor(name='John Doe')]]
    )
    repository: HttpUrl = Field(
        description='The link to the source code of the plugin.',
        examples=[HttpUrl('https://gitlab.heigit.org/climate-action/net_zero')],
    )
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
    purpose: str = Field(
        description='What will this plugin accomplish?',
        examples=['This plugin provides information on a special aspect of climate action.'],
    )
    methodology: str = Field(
        description='How does this plugin achieve its goal?',
        examples=['This plugin uses a combination of data source A and method B to accomplish the purpose.'],
    )
    sources: Optional[List[ArticleSource | IncollectionSource | MiscSource]] = Field(
        description='A list of sources that were used in the process or are related. ',
        examples=[
            [
                {
                    'pages': '14-15',
                    'volume': '2',
                    'journal': 'J. Geophys. Res.',
                    'year': '1954',
                    'title': "Nothing Particular in this Year's History",
                    'author': 'J. G. Smith and H. K. Weston',
                    'ENTRYTYPE': 'article',
                    'ID': 'smit54',
                }
            ]
        ],
        default=None,
    )
    demo_config: DemoConfig = Field(description='Configuration to run a demonstration of a plugin.')
    computation_shelf_life: Optional[timedelta] = Field(
        description='How long are computations valid (at most). Computations will be valid within a fixed time frame '
        'of `shelf_life`. The fix timeframe starts at UNIX TS 0 and renews every `shelf_life`. A time delta of 0 means '
        'no caching while None means indefinite caching.',
        examples=[timedelta(weeks=4)],
        default=timedelta(0),
    )
    assets: Assets = Field(description='Static assets', examples=[Assets(icon='icon.png')])
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
        default=None,
    )
    library_version: Annotated[
        Version,
        PydanticSemver,
        Field(
            description='The climatoology library version, the plugin is using.',
            default=climatoology.__version__,
            examples=[str(Version(1, 2, 3))],  # https://github.com/pydantic/pydantic/issues/12280
        ),
    ]

    @model_validator(mode='after')
    def create_id(self) -> '_Info':
        plugin_id = self.name.lower()
        plugin_id = re.sub(r'[^a-zA-Z-\s]', '', plugin_id)
        plugin_id = re.sub(r'\s', '_', plugin_id)
        self.id = plugin_id
        return self


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


def _convert_bib(sources: Optional[Path] = None) -> Optional[JsonSchemaValue]:
    if sources is None:
        return None
    with open(sources, mode='r') as file:
        return bibtexparser.load(file).get_entry_list()


def generate_plugin_info(
    *,
    name: str,
    authors: List[PluginAuthor],
    icon: Path,
    version: Version,
    concerns: Set[Concern],
    teaser: str,
    purpose: Path,
    methodology: Path,
    demo_config: DemoConfig,
    state: PluginState = PluginState.ACTIVE,
    computation_shelf_life: timedelta = timedelta(0),
    sources: Optional[Path] = None,
) -> _Info:
    """Generate a plugin info object.

    :param name: The full name of the plugin. Try to make it concise.
    :param authors: The list of plugin contributors. The list should be limited to contributors that have invested a
      considerable amount of contributions to the plugin. The list should adhere to the research-paper order i.e. by
      amount of contributions, descending.
    :param icon: The path to an image or icon that can be used to represent the plugin. Make sure the file is committed
      to the repository and HeiGIT has all legal rights to it (without attribution!).
    :param version: The plugin version. Make sure to adhere to [semantic versioning](https://semver.org/)!
    :param concerns: The domains or topics the plugin is tackling.
    :param state: The current development state of the plugin using categories from https://github.com/GIScience/badges.
    :param teaser: A single sentence teaser for the plugins' functionality. The sentence must be between 20 and 150
      characters long, start with an upper case letter and end with a full stop.
    :param purpose: What will this plugin accomplish? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param methodology: How does this plugin achieve its goal? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param computation_shelf_life: How long are computations valid (at most). Computations will be valid within a fixed
      time frame of `shelf_life`. The fix timeframe starts at UNIX TS 0 and renews every `shelf_life`. A time delta of
      0 means no caching while None means indefinite caching.
    :param demo_config: A `DemoConfig` object defining the input parameters, AOI and AOI name for the demo computation.
      The helper function `compose_demo_config` may be used to create this object with a default AOI.
    :param sources: A list of sources that were used in the process or are related. Self-citations are welcome and
      even preferred! Provide a [.bib](https://bibtex.eu/faq/how-do-i-create-a-bib-file-to-manage-my-bibtex-references/)
      file. You can extract such a file from most common bibliography management systems.
    :return: An _Info object that can be used to announce the plugin on the platform.
    """
    assets = Assets(icon=str(icon))
    sources = _convert_bib(sources)

    with open('pyproject.toml', 'rb') as pyproject_toml:
        pyproject = tomllib.load(pyproject_toml)
        repository = pyproject.get('project', {}).get('repository')
        if repository is None:
            raise ValueError(
                'Your pyproject.toml does not contain a repository url or is not adhering to the latest pyproject.toml format: https://python-poetry.org/docs/pyproject/'
            )

    return _Info(
        name=name,
        authors=authors,
        repository=repository,
        version=version,
        concerns=concerns,
        state=state,
        teaser=teaser,
        purpose=purpose.read_text(),
        methodology=methodology.read_text(),
        sources=sources,
        assets=assets,
        demo_config=demo_config,
        computation_shelf_life=computation_shelf_life,
    )


def compose_demo_config(input_parameters: BaseModel, aoi_path: Path = None, aoi_name: str = None) -> DemoConfig:
    """
    Compose the demo config object from the provided components.

    :param input_parameters: the input parameters for the plugin
    :param aoi_path: A path to the file containing the geojson geometry for the area of interest for the demo
    computation. It could be an administrative boundary or an arbitrary geometry. The default is the municipality of
    Heidelberg, but make sure to provide another region if this is not suitable for your plugin. The computation will
    only be done once (and then cached) so the size of the area may not be the biggest concern as long as it can be
    computed within ~30min.
    :param aoi_name: a string for the display name of the demo computation
    """
    if not aoi_path:
        aoi_path = DEMO_AOI_PATH
        if aoi_name:
            log.warning('You provided a `aoi_name` but not `aoi_path`. The default demo AOI and name will be used')
        aoi_name = 'Heidelberg Demo'

    if aoi_path and not aoi_name:
        raise ValueError('You provided `aoi_path` but no `name`. Please include a `name` for the demo AOI')

    demo_aoi = geojson_pydantic.MultiPolygon(**json.loads(aoi_path.read_text()))
    return DemoConfig(aoi=demo_aoi, params=input_parameters.model_dump(), name=aoi_name)
