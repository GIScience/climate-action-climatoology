import base64
import re
from abc import ABC, abstractmethod
from enum import Enum
from io import BytesIO
from pathlib import Path
from typing import Optional, List, Generic, TypeVar, Dict, Type, Any, get_origin, get_args, final

import bibtexparser
from PIL import Image
from pydantic import BaseModel, field_validator, Field, model_validator
from semver import Version

import climatoology
from climatoology.base.artifact import Artifact
from climatoology.base.computation import ComputationResources


class Concern(Enum):
    """Keywords that group plugins by topic."""
    CLIMATE_ACTION__GHG_EMISSION = 'ghg_emission'
    CLIMATE_ACTION__MITIGATION = 'mitigation'
    CLIMATE_ACTION__ADAPTION = 'adaption'

    SUSTAINABILITY__WASTE = 'waste'


class Info(BaseModel, extra='forbid'):
    """A dataclass to provide the basic information about a plugin."""

    name: str = Field(description='A short and concise name that can be used in the UI.',
                      examples=['The Plugin'])
    icon: str = Field(description='An image or icon that can be used in the UI in the form of a data URL. If the '
                                  'input is a path, it will be automatically converted. Make sure the file is '
                                  'committed to the repository and you have all rights to use it.',
                      examples=['data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBk'
                                'SEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/2wBDAQkJCQwLDBgNDRgyIRwhMjIyM'
                                'jIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjIyMjL/wAARCAACAAIDASIAAhEBAxE'
                                'B/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhM'
                                'UEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmN'
                                'kZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW1'
                                '9jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/8QAHwEAAwEBAQEBAQEBAQAAAAAAAAECAwQFBgcICQoL/8QAtREAAgE'
                                'CBAQDBAcFBAQAAQJ3AAECAxEEBSExBhJBUQdhcRMiMoEIFEKRobHBCSMzUvAVYnLRChYkNOEl8RcYGRomJygpK'
                                'jU2Nzg5OkNERUZHSElKU1RVVldYWVpjZGVmZ2hpanN0dXZ3eHl6goOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKm'
                                'qsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4uPk5ebn6Onq8vP09fb3+Pn6/9oADAMBAAIRAxEAPwDwa5ubi'
                                '5u5p555ZZpHZ3kdyzMxOSST1JPeiiigD//Z'])
    version: str = Field(description='The plugin version. You can provide a Version object.',
                         examples=[str(Version(0, 0, 1)), 'alpha-centauri'])
    concerns: List[Concern] = Field(description='A set of keywords that can be used to group multiple plugins.',
                                    examples=[[Concern.CLIMATE_ACTION__GHG_EMISSION,
                                               Concern.CLIMATE_ACTION__MITIGATION]])
    purpose: str = Field(description='What will this plugin accomplish?',
                         examples=['This plugin provides information on a special aspect of climate action.'])
    methodology: str = Field(description='How does the operator achieve its goal?',
                             examples=['This plugin uses a combination of data source A and method B to accomplish '
                                       'the purpose.'])
    sources: Optional[List[dict]] = Field(description='A list of sources that were used in the process or are related. '
                                                      'Self-citations are welcome and even preferred! You can provide '
                                                      'a path to a bib-tex file that will then be parsed '
                                                      'automatically.',
                                          examples=[[{
                                              'pages': '14-15',
                                              'volume': '2',
                                              'journal': 'J. Geophys. Res.',
                                              'year': '1954',
                                              'title': "Nothing Particular in this Year's History",
                                              'author': 'J. G. Smith and H. K. Weston',
                                              'ENTRYTYPE': 'article',
                                              'ID': 'smit54'
                                          }]],
                                          default=None)
    plugin_id: Optional[str] = Field(description='Do not set! It will be overridden with the cleaned name.',
                                     examples=['the_plugin_001'],
                                     default=None)
    operator_schema: Optional[dict] = Field(description='Do not set! It will be overridden by the plugin with the '
                                                        'schematic description of the parameters necessary to '
                                                        'initiate a computation.',
                                            examples=[{'properties': {
                                                'bool': {
                                                    'description': 'A required boolean parameter.',
                                                    'examples': [
                                                        True
                                                    ],
                                                    'title': 'Boolean Input',
                                                    'type': 'boolean'
                                                }, 'required': [
                                                    'bool',
                                                ],
                                                'title': 'ComputeInput',
                                                'type': 'object'}}],
                                            default=None)
    library_version: str = Field(description='Do not set!',
                                 default=climatoology.__version__)

    @field_validator('version', mode='before')
    def _convert_version(cls, version: Any) -> str:
        if isinstance(version, Version):
            return str(version)
        return version

    @classmethod
    def _verify_icon(cls, icon: Path) -> None:
        image = Image.open(icon)
        image.verify()

    @field_validator('icon', mode='before')
    def _convert_icon(cls, icon: Any) -> str:
        if isinstance(icon, Path):
            Info._verify_icon(icon)
            image = Image.open(icon)
            image.thumbnail((500, 500))
            buffered = BytesIO()
            image.save(buffered, format='JPEG')
            buffered.seek(0)
            data_url = base64.b64encode(buffered.getvalue()).decode('UTF-8')
            return f'data:image/jpeg;base64,{data_url}'
        return icon

    @field_validator('sources', mode='before')
    def _convert_bib(cls, sources: Any) -> dict:
        if isinstance(sources, Path):
            with open(sources, mode='r') as file:
                return bibtexparser.load(file).get_entry_list()
        return sources

    @field_validator('concerns', mode='before')
    def _convert_concerns(cls, concerns: Any) -> List[Concern]:
        if isinstance(concerns, List):
            return [Concern(x) for x in concerns]
        return concerns

    @model_validator(mode='after')
    def create_id(self) -> 'Info':
        assert len(re.findall('[^a-zA-Z- ]', self.name)) == 0, ('Special characters and numbers are not allowed '
                                                                'in the name.')
        self.plugin_id, _ = re.subn('[- ]', '_', self.name.lower())
        return self


T_co = TypeVar('T_co', bound=BaseModel, covariant=True)


class Operator(ABC, Generic[T_co]):
    """Climate Action indicator logic.

    The operator provides the core functionality for the plugin.
     Each application that serves a plugin is required to implement its own operator.
    """

    _model: Optional[Type[T_co]] = None

    @classmethod
    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Verify whether subclass follows the model contract defined by the
        Operator superclass

        :param kwargs:
        :return:
        """

        super().__init_subclass__(**kwargs)
        for base in cls.__orig_bases__:  # type: ignore[attr-defined]
            origin = get_origin(base)
            if origin is None or not issubclass(origin, Operator):
                continue
            type_arg = get_args(base)[0]
            if not isinstance(type_arg, TypeVar):
                cls._model = type_arg
                return
        assert cls._model, 'Could not initialise the compute input type model. Did you properly subtype your operator?'

    @final
    def info_enriched(self) -> Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        info = self.info()
        info.operator_schema = self._model.model_json_schema()
        return info

    @abstractmethod
    def info(self) -> Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        pass

    @final
    def compute_unsafe(self, resources: ComputationResources, params: Dict) -> List[Artifact]:
        """
        Translated the incoming parameters to a declared pydantic model,
        validates input and runs the compute procedure.

        :param resources: computation ephemeral resources
        :param params: computation configuration parameters
        :return:
        """

        validate_params = self._model(**params)
        return self.compute(resources, validate_params)

    @abstractmethod
    def compute(self, resources: ComputationResources, params: T_co) -> List[Artifact]:
        """Generate an operator-specific report.

        A report is made up of a set of artifacts that can be displayed by a client.

        :param resources: computation ephemeral resources
        :param params: computation parameters in the form of the declared pydantic module
        :return: list of artifacts (files) produced by the operator
        """
        pass
