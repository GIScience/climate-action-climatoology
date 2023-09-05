from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional, List
from uuid import UUID

import bibtexparser
import marshmallow_dataclass
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


class Operator(ABC):
    """Climate Action indicator logic.

    The operator provides the core functionality for the plugin.
     Each application that serves a plugin is required to implement its own operator.
    """

    @abstractmethod
    def info(self) -> Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        pass

    @abstractmethod
    def report(self, params: dict) -> List[Artifact]:
        """Generate an operator-specific report.

        A report is made up of a set of artifacts that can be displayed by a client.

        :param params: report creation parameters
        :return: list of artifacts (files) produced by the operator
        """
        pass
