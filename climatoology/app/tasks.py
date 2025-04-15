import datetime
import logging
import tempfile
from pathlib import Path
from typing import List

import geojson_pydantic
import shapely
from celery import Task
from shapely import set_srid
from sqlalchemy.orm import Session

from climatoology.base.artifact import ArtifactModality, _Artifact
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationScope
from climatoology.base.event import ComputationState
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import COMPUTATION_INFO_FILENAME, Storage, ComputationInfo, PluginBaseInfo

log = logging.getLogger(__name__)


class CAPlatformComputeTask(Task):
    """Climate Action Platform Task for computations.

    It's responsible for handling user input and result storage.
    The main computation logic and workload is handled by the Operator.
    """

    def __init__(self, operator: BaseOperator, storage: Storage, backend_db: BackendDatabase):
        self.operator = operator
        self.storage = storage
        self.name = 'compute'
        self.backend_db = backend_db
        self.sessions = {}

        self.plugin_id = operator.info_enriched.plugin_id

        log.info(f'Compute task for {self.plugin_id} initialised')

    def before_start(self, task_id, args, kwargs):
        self.sessions[task_id] = Session(self.backend_db.engine)
        super().before_start(task_id, args, kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        session = self.sessions.pop(task_id)
        session.close()
        super().after_return(status, retval, task_id, args, kwargs, einfo)

    @property
    def session(self):
        return self.sessions[self.request.id]

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
        self.update_state(task_id=self.request.correlation_id, state='STARTED')
        log.info(f'Acquired compute request ({correlation_uuid}) with id {self.request.id}')

        aoi = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](**aoi)

        # through difficult typing above we know it's a MultiPolygon but the type checker cannot know
        # noinspection PyTypeChecker
        aoi_shapely_geom: shapely.MultiPolygon = shapely.geometry.shape(context=aoi.geometry)
        aoi_shapely_geom = set_srid(geometry=aoi_shapely_geom, srid=4326)

        validated_params = self.operator.validate_params(params)
        log.debug(f'Validated compute parameters for request ({correlation_uuid}): {validated_params}')

        with ComputationScope(correlation_uuid) as resources:
            artifacts = self.operator.compute_unsafe(
                resources=resources, aoi=aoi_shapely_geom, aoi_properties=aoi.properties, params=validated_params
            )
            artifact_errors = resources.artifact_errors
            plugin_artifacts = [
                _Artifact(correlation_uuid=correlation_uuid, **artifact.model_dump(exclude={'correlation_uuid'}))
                for artifact in artifacts
            ]
            self.storage.save_all(plugin_artifacts)

        computation_info = ComputationInfo(
            correlation_uuid=correlation_uuid,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
            params=validated_params.model_dump(),
            aoi=aoi,
            artifacts=plugin_artifacts,
            plugin_info=PluginBaseInfo(
                plugin_id=self.operator.info_enriched.plugin_id,
                plugin_version=self.operator.info_enriched.version,
            ),
            status=ComputationState.SUCCESS,
            artifact_errors=artifact_errors,
        )
        self._save_computation_info(computation_info=computation_info)

        log.debug(f'{correlation_uuid} successfully computed')
        encoded_result = [artifact.model_dump(mode='json') for artifact in plugin_artifacts]
        return encoded_result
