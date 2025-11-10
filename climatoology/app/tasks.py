import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import geojson_pydantic
import shapely
from celery import Task
from celery.signals import task_revoked
from shapely import set_srid

from climatoology.base.artifact import COMPUTATION_INFO_FILENAME, ArtifactEnriched, ArtifactModality
from climatoology.base.baseoperator import AoiProperties, BaseOperator
from climatoology.base.computation import ComputationScope
from climatoology.base.logging import get_climatoology_logger
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import ComputationInfo, Storage
from climatoology.utility.exception import InputValidationError

log = get_climatoology_logger(__name__)


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

        self.plugin_id = operator.info_enriched.id

        log.info(f'Compute task for {self.plugin_id} initialised')

    def _save_computation_info(self, computation_info: ComputationInfo) -> str:
        with tempfile.TemporaryDirectory() as temp_dir:
            with open(Path(temp_dir) / COMPUTATION_INFO_FILENAME, 'x') as out_file:
                log.debug(f'Writing metadata file {out_file}')

                out_file.write(computation_info.model_dump_json(indent=None))

                result = ArtifactEnriched(
                    name='Computation Info',
                    rank=sys.maxsize,
                    modality=ArtifactModality.COMPUTATION_INFO,
                    filename=COMPUTATION_INFO_FILENAME,
                    summary=f'Computation information of correlation_uuid {computation_info.correlation_uuid}',
                    correlation_uuid=computation_info.correlation_uuid,
                )
                log.debug(f'Returning Artifact: {result.model_dump()}.')

            return self.storage.save(result, file_dir=Path(temp_dir))

    def run(self, aoi: dict, params: dict) -> dict:
        correlation_uuid: UUID = self.request.correlation_id  # Typing seems wrong
        try:
            self.update_state(task_id=self.request.correlation_id, state='STARTED')
            log.debug('Acquired compute request')

            aoi = geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties](**aoi)

            # through difficult typing above we know it's a MultiPolygon but the type checker cannot know
            # noinspection PyTypeChecker
            aoi_shapely_geom: shapely.MultiPolygon = shapely.geometry.shape(context=aoi.geometry)
            aoi_shapely_geom = set_srid(geometry=aoi_shapely_geom, srid=4326)

            validated_params = self.operator.validate_params(params)
            self.backend_db.add_validated_params(
                correlation_uuid=correlation_uuid, params=validated_params.model_dump(mode='json')
            )
            log.debug(f'Validated compute parameters to: {validated_params}')

            with ComputationScope(correlation_uuid) as resources:
                artifacts = self.operator.compute_unsafe(
                    resources=resources, aoi=aoi_shapely_geom, aoi_properties=aoi.properties, params=validated_params
                )
                artifact_errors = resources.artifact_errors
                self.storage.save_all(artifacts, file_dir=resources.computation_dir)

            computation_info = self.backend_db.read_computation(correlation_uuid=correlation_uuid)
            computation_info.timestamp = datetime.now(UTC).replace(tzinfo=None)
            computation_info.params = validated_params.model_dump()
            computation_info.artifacts = artifacts
            computation_info.artifact_errors = artifact_errors

            self._save_computation_info(computation_info=computation_info)
            self.backend_db.update_successful_computation(
                computation_info=computation_info, invalidate_cache=bool(computation_info.artifact_errors)
            )

            output = computation_info.model_dump(mode='json')

            log.debug('Successfully completed')
        except Exception as e:
            # Note that the on_success and on_failure callbacks provided by celery.Task class will create an
            # inconsistent DB-state where celery is already aware of the result while our custom result tables are not.
            self.backend_db.update_failed_computation(
                correlation_uuid=correlation_uuid, failure_message=str(e), cache=isinstance(e, InputValidationError)
            )
            raise e

        return output


@task_revoked.connect
def uncache_revoked_task_on_worker(**kwargs):
    correlation_uuid = kwargs['request'].id
    sender = kwargs['sender']
    sender.backend_db.update_failed_computation(correlation_uuid=correlation_uuid, failure_message=None, cache=False)
