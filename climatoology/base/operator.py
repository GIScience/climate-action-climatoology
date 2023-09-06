from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, List, Generic, TypeVar, Dict, Type, Any, get_origin, get_args, final
from uuid import UUID

import bibtexparser
import marshmallow_dataclass
from pydantic import BaseModel
from semver import Version


@dataclass
class Info:

    def __init__(self, name: str, version: Version, purpose: str, methodology: str, sources_bib: Optional[Path] = None):
        self.name: str = name
        self.version: str = str(version)
        self.purpose: str = purpose
        self.methodology: str = methodology
        self.sources = {}

        if sources_bib is not None:
            with open(sources_bib, mode='r') as file:
                self.sources: dict = bibtexparser.load(file).get_entry_dict()


info_schema = marshmallow_dataclass.class_schema(Info)()


class ArtifactModality(Enum):
    """Available artifact types."""
    TEXT = 'TEXT'
    TABLE = 'TABLE'
    MAP_LAYER = 'MAP_LAYER'
    IMAGE = 'IMAGE'
    URL = 'URL'


@dataclass
class Artifact:
    correlation_uuid: UUID
    modality: ArtifactModality
    file_path: Path


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

    @abstractmethod
    def info(self) -> Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        pass

    @final
    def report_unsafe(self, params: Dict) -> List[Artifact]:
        """
        Translated the incoming parameters to a declared pydantic model,
        validates input and runs the report procedure.

        :param params:
        :return:
        """

        validate_params = self._model(**params)
        return self.report(validate_params)

    @abstractmethod
    def report(self, params: T_co) -> List[Artifact]:
        """Generate an operator-specific report.

        A report is made up of a set of artifacts that can be displayed by a client.

        :param params: report creation parameters in the form of the declared pydantic module
        :return: list of artifacts (files) produced by the operator
        """
        pass
