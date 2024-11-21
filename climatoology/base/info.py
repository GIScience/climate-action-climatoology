import re
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Optional, List, Set

import bibtexparser
from PIL import Image
from pydantic import BaseModel, Field, HttpUrl, model_validator
from pydantic.json_schema import JsonSchemaValue
from semver import Version

import climatoology


class Concern(Enum):
    """Keywords that group plugins by topic."""

    CLIMATE_ACTION__GHG_EMISSION = 'ghg_emission'
    CLIMATE_ACTION__MITIGATION = 'mitigation'
    CLIMATE_ACTION__ADAPTION = 'adaption'

    MOBILITY_PEDESTRIAN = 'pedestrian'
    MOBILITY_CYCLING = 'cycling'

    SUSTAINABILITY__WASTE = 'waste'


class PluginAuthor(BaseModel):
    name: str = Field(description='The author name.', examples=['John Doe'])
    affiliation: Optional[str] = Field(
        description="The author's affiliation statement. Leave blank if you are a " 'HeiGIT member.',
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


class Assets(BaseModel):
    """Static data linked to the plugin that should be stored in the object store."""

    icon: str = Field(description='The icon asset', examples=['icon.jpeg'])


class _Info(BaseModel, extra='forbid'):
    """A dataclass to provide the basic information about a plugin."""

    name: str = Field(description='The full name of the plugin.', examples=['The Plugin'])
    authors: List[PluginAuthor] = Field(description='A list of plugin contributors.')
    version: str = Field(
        description='The plugin version.',
        examples=[str(Version(0, 0, 1)), 'alpha-centauri'],
    )
    concerns: Set[Concern] = Field(
        description='The domains or topics the plugin is tackling. It can be used as a set of keywords that '
        'group multiple plugins.',
        examples=[{Concern.CLIMATE_ACTION__GHG_EMISSION, Concern.CLIMATE_ACTION__MITIGATION}],
    )
    purpose: str = Field(
        description='What will this plugin accomplish?',
        examples=['This plugin provides information on a special aspect of climate action.'],
    )
    methodology: str = Field(
        description='How does this plugin achieve its goal?',
        examples=['This plugin uses a combination of data source A and method B to accomplish the purpose.'],
    )
    sources: Optional[List[dict]] = Field(
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
    assets: Assets = Field(description='Static assets', examples=[Assets(icon='icon.jpeg')])
    plugin_id: Optional[str] = Field(
        description='A cleaned plugin name.',
        examples=['the_plugin_001'],
        default=None,
    )
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
    library_version: str = Field(
        description='The climatoology library version, the plugin is using.', default=str(climatoology.__version__)
    )

    @model_validator(mode='after')
    def create_id(self) -> '_Info':
        assert (
            len(re.findall('[^a-zA-Z- ]', self.name)) == 0
        ), 'Special characters and numbers are not allowed in the name.'
        self.plugin_id, _ = re.subn('[- ]', '_', self.name.lower())
        return self


def _verify_icon(icon: Path) -> None:
    image = Image.open(icon)
    image.verify()


def _convert_icon_to_thumbnail(icon: Path) -> BytesIO:
    _verify_icon(icon)
    image = Image.open(icon)
    image.thumbnail((500, 500))
    buffered = BytesIO()
    image.save(buffered, format='JPEG')
    buffered.seek(0)
    return buffered


def _convert_bib(sources: Path = None) -> Optional[JsonSchemaValue]:
    if sources is None:
        return None
    with open(sources, mode='r') as file:
        return bibtexparser.load(file).get_entry_list()


def generate_plugin_info(
    name: str,
    authors: List[PluginAuthor],
    icon: Path,
    version: Version,
    concerns: Set[Concern],
    purpose: Path,
    methodology: Path,
    sources: Path = None,
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
    :param purpose: What will this plugin accomplish? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param methodology: How does this plugin achieve its goal? Provide a text file that can have
      [markdown](https://www.markdownguide.org/) formatting.
    :param sources: A list of sources that were used in the process or are related. Self-citations are welcome and
      even preferred! Provide a [.bib](https://bibtex.eu/faq/how-do-i-create-a-bib-file-to-manage-my-bibtex-references/)
      file. You can extract such a file from most common bibliography management systems.
    :return: An _Info object that can be used to announce the plugin on the platform.
    """
    assets = Assets(icon=str(icon))
    return _Info(
        name=name,
        authors=authors,
        version=str(version),
        concerns=concerns,
        purpose=purpose.read_text(),
        methodology=methodology.read_text(),
        sources=_convert_bib(sources),
        assets=assets,
    )
