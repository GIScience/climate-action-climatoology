import datetime
import logging
import tempfile
from pathlib import Path
from typing import Optional, List
from uuid import UUID

import geojson_pydantic
import shapely
from celery import Task
from pydantic import BaseModel
from shapely import set_srid

from climatoology.base.artifact import _Artifact, ArtifactModality
from climatoology.base.baseoperator import BaseOperator, AoiProperties
from climatoology.base.computation import ComputationScope
from climatoology.base.event import ComputeCommandStatus
from climatoology.store.object_store import Storage, COMPUTATION_INFO_FILENAME
from climatoology.utility.exception import InputValidationError

log = logging.getLogger(__name__)


class PluginBaseInfo(BaseModel):
    plugin_id: str
    plugin_version: str


class ComputationInfo(BaseModel, extra='forbid'):
    correlation_uuid: UUID
    timestamp: datetime.datetime
    params: dict
    aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties]
    artifacts: Optional[List[_Artifact]] = []
    plugin_info: PluginBaseInfo
    status: ComputeCommandStatus
    message: Optional[str] = '-'


class CAPlatformComputeTask(Task):
    """Climate Action Platform Task for computations.

    It's responsible for handling user input and result storage.
    The main computation logic and workload is handled by the Operator.
    """

    def __init__(self, operator: BaseOperator, storage: Storage):
        self.operator = operator
        self.storage = storage
        self.name = 'compute'

        self.plugin_id = operator.info_enriched.plugin_id

        log.info(f'Compute task for {self.plugin_id} initialised')

    def _save_computation_info(self, computation_info: ComputationInfo) -> str:
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(Path(temp_dir) / COMPUTATION_INFO_FILENAME, 'x') as out_file:
                log.debug(f'Writing metadata file {out_file}')

                out_file.write(computation_info.model_dump_json(indent=None))

                result = _Artifact(
                    name='Computation Info',
                    modality=ArtifactModality.COMPUTATION_INFO,
                    file_path=Path(out_file.name),
                    summary=f'Computation information of correlation_uuid {computation_info.correlation_uuid}',
                    correlation_uuid=computation_info.correlation_uuid,
                )
                log.debug(f'Returning Artifact: {result.model_dump()}.')

                return self.storage.save(result)

    def run(self, aoi: dict, params: dict) -> List[dict]:
        correlation_uuid = self.request.correlation_id

        aoi = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](**aoi)

        aoi_shapely_geom: shapely.MultiPolygon = shapely.geometry.shape(context=aoi.geometry)
        aoi_shapely_geom = set_srid(geometry=aoi_shapely_geom, srid=4326)

        try:
            log.info(f'Acquired compute request ({correlation_uuid}) with id {self.request.id}')

            log.debug(f'Computing with parameters {params}')

            with ComputationScope(correlation_uuid) as resources:
                raw_artifacts = self.operator.compute_unsafe(
                    resources=resources, aoi=aoi_shapely_geom, aoi_properties=aoi.properties, params=params
                )
                artifacts = list(filter(None, raw_artifacts))

                for artifact in artifacts:
                    assert (
                        artifact.modality != ArtifactModality.COMPUTATION_INFO
                    ), 'Computation-info files are not allowed as plugin result'

                plugin_artifacts = [
                    _Artifact(correlation_uuid=correlation_uuid, **artifact.model_dump(exclude={'correlation_uuid'}))
                    for artifact in artifacts
                ]
                self.storage.save_all(plugin_artifacts)

                computation_info = ComputationInfo(
                    correlation_uuid=correlation_uuid,
                    timestamp=datetime.datetime.now(datetime.timezone.utc),
                    params=params,
                    aoi=aoi,
                    artifacts=plugin_artifacts,
                    plugin_info=PluginBaseInfo(
                        plugin_id=self.operator.info_enriched.plugin_id,
                        plugin_version=self.operator.info_enriched.version,
                    ),
                    status=ComputeCommandStatus.COMPLETED,
                )
                self._save_computation_info(computation_info=computation_info)

            log.debug(f'{correlation_uuid} successfully computed')
            encoded_result = [artifact.model_dump(mode='json') for artifact in plugin_artifacts]
            return encoded_result
        except InputValidationError as e:
            log.warning(f'Input validation failed for correlation id {correlation_uuid}', exc_info=e)
            raise e
        except Exception as e:
            log.warning(f'Computation failed for correlation id {correlation_uuid}', exc_info=e)
            raise e


class CAPlatformInfoTask(Task):
    """Climate Action Platform Task to get operator capabilities.

    It's responsible for forwarding info requests to the user.
    The info content is provided by the Operator.
    """

    def __init__(self, operator: BaseOperator, storage: Storage, overwrite_assets: bool):
        self.name = 'info'
        self.operator = operator
        self.storage = storage
        self.info = operator.info_enriched

        assets = self.storage.synch_assets(
            plugin_id=self.info.plugin_id,
            plugin_version=self.info.version,
            assets=self.info.assets,
            overwrite=overwrite_assets,
        )
        self.info.assets = assets

        log.info(f'Info task for {self.info.plugin_id} initialised')

    def run(self) -> dict:
        correlation_uuid = self.request.correlation_id
        log.debug(f'Acquired info request ({correlation_uuid})')

        return self.info.model_dump(mode='json')
