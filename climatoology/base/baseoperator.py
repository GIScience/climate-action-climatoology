import logging
import uuid
from abc import ABC, abstractmethod
from contextlib import contextmanager
from functools import cached_property
from typing import Any, ContextManager, Dict, Generic, List, Optional, Type, TypeVar, final, get_args, get_origin

import shapely
from pydantic import BaseModel, Field

import climatoology
from climatoology.base import T_co
from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.computation import ComputationResources
from climatoology.base.info import _Info
from climatoology.utility.exception import InputValidationError

log = logging.getLogger(__name__)


class AoiProperties(BaseModel):
    name: str = Field(
        title='Name',
        description='The name of the area of interest i.e. a human readable description.',
        examples=['Heidelberg'],
    )
    id: str = Field(
        title='ID',
        description='A unique identifier of the area of interest.',
        examples=[str(uuid.uuid4())],
    )


class BaseOperator(ABC, Generic[T_co]):
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
            if origin is None or not issubclass(origin, BaseOperator):
                continue
            type_arg = get_args(base)[0]
            if not isinstance(type_arg, TypeVar):
                cls._model = type_arg
                return
        assert cls._model, 'Could not initialise the compute input type model. Did you properly subtype your operator?'
        log.debug('Operator initialised')

    def __init__(self) -> None:
        forbidden_fields = ('aoi', 'aoi_properties')
        assert all(
            k not in self._model.model_fields for k in forbidden_fields
        ), f'The plugin input parameters cannot contain fields named any of {forbidden_fields}'

    @final
    @cached_property
    def info_enriched(self) -> _Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        info = self.info()
        info.operator_schema = self._model.model_json_schema()
        info.library_version = str(climatoology.__version__)
        log.debug(f'{info.name} info constructed')
        return info

    @abstractmethod
    def info(self) -> _Info:
        """Describe the operators' purpose, functionality, methodology and sources.

        :return: operator info
        """
        pass

    @final
    def validate_params(self, params: Dict) -> T_co:
        """
        Translate the incoming parameters to the declared pydantic model and validate them.

        :param params: computation configuration parameters
        :return: the validated parameters
        """
        log.debug('Validating input parameters')
        try:
            return self._model.model_validate(params)
        except Exception as e:
            raise InputValidationError('The given user input is invalid') from e

    @final
    def compute_unsafe(
        self, resources: ComputationResources, aoi: shapely.MultiPolygon, aoi_properties: AoiProperties, params: T_co
    ) -> List[_Artifact]:
        """
        Runs the compute procedure, checks and filters the returned artifacts.

        :param resources: computation ephemeral resources
        :param aoi: Area of interest for the computation
        :param aoi_properties: Properties of the area of interest for the computation
        :param params: computation configuration parameters
        :return: a list of artifacts
        """
        logging.debug(f'Beginning computation for correlation_uuid {resources.correlation_uuid}')
        artifacts = self.compute(resources=resources, aoi=aoi, aoi_properties=aoi_properties, params=params)

        artifacts = list(filter(None, artifacts))
        assert len(artifacts) > 0, 'The computation returned no results.'
        for artifact in artifacts:
            assert (
                artifact.modality != ArtifactModality.COMPUTATION_INFO
            ), 'Computation-info files are not allowed as plugin result'

        return artifacts

    @abstractmethod
    def compute(
        self, resources: ComputationResources, aoi: shapely.MultiPolygon, aoi_properties: AoiProperties, params: T_co
    ) -> List[_Artifact]:
        """Generate an operator-specific report.

        A report is made up of a set of artifacts that can be displayed by a client.

        :param resources: computation ephemeral resources
        :param aoi: the requested ara of interest as a shapely MultiPolygon with SRID 4326
        :param aoi_properties: properties related to and common for all AOIs
        :param params: computation parameters in the form of the declared pydantic module
        :return: list of artifacts (files) produced by the operator
        """
        pass

    @final
    @staticmethod
    @contextmanager
    def catch_exceptions(indicator_name: str, resources: ComputationResources) -> ContextManager[Any]:
        """Catch any errors in the statements within this context manager, without failing. If the inner code fails,
        save the `indicator_name` to `resources.artifact_errors`. Use it
        as `with catch_exceptions('ghg_budget', resources=resources):

        :param indicator_name: the indicator name, used to provide context to the error messages in the front end.
        :param resources: the `ComputationResources` containing computation-specific attributes.
        """
        try:
            yield
        except Exception as e:
            resources.artifact_errors.append(indicator_name)
            log.error(
                f'{indicator_name} computation failed for correlation id {resources.correlation_uuid}', exc_info=e
            )
