import shutil
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import UUID

import geojson_pydantic
from pydantic import BaseModel, ConfigDict, Field
from semver import Version

from climatoology.base.artifact import ArtifactEnriched, ArtifactModality
from climatoology.base.event import ComputationState
from climatoology.base.plugin_info import PluginBaseInfo


class ComputationResources(BaseModel):
    """A directory computation resource."""

    correlation_uuid: UUID
    computation_dir: Path
    artifact_errors: dict[str, str] = {}


class ComputationScope:
    """Context manager to isolate working environments for computations."""

    def __init__(self, correlation_uuid: UUID):
        self.resources = ComputationResources(
            computation_dir=Path(tempfile.mkdtemp(prefix=str(correlation_uuid))), correlation_uuid=correlation_uuid
        )

    def __enter__(self):
        return self.resources

    def __exit__(self, *args):
        shutil.rmtree(self.resources.computation_dir)


class AoiProperties(BaseModel):
    model_config = ConfigDict(extra='allow')

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


class ComputationInfo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    correlation_uuid: UUID = Field(description='The unique identifier of the computation.', examples=[uuid.uuid4()])
    request_ts: datetime = Field(
        description='The timestamp at which the computation was requested.', examples=[datetime.now()]
    )
    deduplication_key: UUID = Field(
        description='A key identifying unique contributions in terms of content. It is a combination of multiple '
        'fields of the info that are used to deduplicate computations in combination with the '
        '`cache_epoch`.',
        examples=[uuid.uuid4()],
    )
    cache_epoch: Optional[int] = Field(
        description='The cache epoch identifies fixed time spans within which computations are '
        'valid. It can be used in combination with the `deduplication_key` to deduplicate non-expired computations. ',
        examples=[1234],
    )
    valid_until: datetime = Field(description='The human readable form of the `cache_epoch`', examples=[datetime.now()])
    params: Optional[dict] = Field(
        description='The final parameters used for the computation.',
        examples=[{'param_a': 1, 'optional_param_b': 'b'}],
    )
    requested_params: dict = Field(
        description='The raw parameters that were requested by the client', examples=[{'param_a': 1}]
    )
    aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties] = Field(
        description='The target area of interest of the computation.',
        examples=[
            geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](
                **{
                    'type': 'Feature',
                    'properties': {'name': 'test_aoi', 'id': 'test_aoi_id'},
                    'geometry': {
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
                    },
                }
            )
        ],
    )
    artifacts: List[ArtifactEnriched] = Field(
        description='List of artifacts produced by this computation.',
        examples=[
            ArtifactEnriched(
                name='Artifact One',
                modality=ArtifactModality.MARKDOWN,
                filename='example_file.md',
                summary='An example artifact.',
                correlation_uuid=uuid.uuid4(),
                rank=0,
            )
        ],
        default=[],
    )
    plugin_info: PluginBaseInfo = Field(
        description='Basic information on the plugin that produced the computation.',
        examples=[
            PluginBaseInfo(id='example_plugin', version=Version(0, 0, 1)),
        ],
    )
    status: Optional[ComputationState] = Field(
        description='The current status of the computation.', examples=[ComputationState.SUCCESS], default=None
    )
    message: Optional[str] = Field(description='A message accompanying the computation.', examples=[None], default=None)
    artifact_errors: dict[str, str] = Field(
        description='A dictionary of artifact names that were not computed successfully during the computation, with error messages if applicable.',
        examples=[{'First Indicator': 'Start date must be before end date', 'Last Indicator': ''}],
        default={},
    )
