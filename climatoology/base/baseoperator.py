import logging
import uuid
from abc import ABC, abstractmethod
from functools import cached_property
from typing import Optional, List, Generic, TypeVar, Dict, Type, Any, get_origin, get_args, final

import shapely
from pydantic import BaseModel
from pydantic import Field

import climatoology
from climatoology.base.artifact import _Artifact
from climatoology.base.computation import ComputationResources
from climatoology.base.info import _Info
from climatoology.utility.exception import InputValidationError

log = logging.getLogger(__name__)

T_co = TypeVar('T_co', bound=BaseModel, covariant=True)


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
    def compute_unsafe(
        self, resources: ComputationResources, aoi: shapely.MultiPolygon, aoi_properties: AoiProperties, params: Dict
    ) -> List[_Artifact]:
        """
        Translated the incoming parameters to a declared pydantic model,
        validates input and runs the compute procedure.

        :param resources: computation ephemeral resources
        :param aoi: Area of interest for the computation
        :param aoi_properties: Properties of the area of interest for the computation
        :param params: computation configuration parameters
        :return:
        """
        try:
            validate_params = self._model.model_validate(params)
        except Exception as e:
            raise InputValidationError('The given user input is invalid') from e
        logging.debug(f'Compute parameters of correlation_uuid {resources.correlation_uuid} validated')

        artifacts = self.compute(resources=resources, aoi=aoi, aoi_properties=aoi_properties, params=validate_params)

        artifacts = list(filter(None, artifacts))
        assert len(artifacts) > 0, 'The computation returned no results.'

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
