import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import plotly
import shapely
from celery import Task
from celery.signals import task_revoked
from pydantic import BaseModel
from shapely import MultiPolygon

from climatoology.base.artifact import COMPUTATION_INFO_FILENAME, ArtifactEnriched, ArtifactModality
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import (
    AoiFeatureModel,
    AoiProperties,
    ComputationInfo,
    ComputationPluginInfo,
    ComputationResources,
    ComputationScope,
    StandAloneComputationInfo,
)
from climatoology.base.exception import InputValidationError
from climatoology.base.i18n import set_language
from climatoology.base.logging import get_climatoology_logger
from climatoology.base.plugin_info import DEFAULT_LANGUAGE
from climatoology.base.utils import shapely_from_geojson_pydantic
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import Storage

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

            (computation_info_store_id,) = self.storage.save(result, file_dir=Path(temp_dir))
            return computation_info_store_id

    def run(self, *, aoi: dict, params: dict, lang: str = DEFAULT_LANGUAGE, **kwargs: Any) -> dict:
        correlation_uuid = UUID(self.request.correlation_id)

        if kwargs:
            log.warning(
                f'The following arguments were included in the compute request but will not be handled: {kwargs}'
            )

        try:
            self.update_state(task_id=str(correlation_uuid), state='STARTED')
            log.debug('Acquired compute request')

            set_language(lang=lang, localisation_dir=self.operator.info_enriched.assets.localisation_directory)

            aoi_feature = AoiFeatureModel(**aoi)

            # through difficult typing above we know it's a MultiPolygon but the type checker cannot know
            # noinspection PyTypeChecker
            aoi_shapely_geom: MultiPolygon = shapely_from_geojson_pydantic(geojson_geom=aoi_feature.geometry)

            validated_params = self.operator.validate_params(params)
            self.backend_db.add_validated_params(
                correlation_uuid=correlation_uuid, params=validated_params.model_dump(mode='json')
            )
            log.debug(f'Validated compute parameters to: {validated_params}')

            with ComputationScope(correlation_uuid) as resources:
                artifacts = self.operator.compute_unsafe(
                    resources=resources,
                    aoi=aoi_shapely_geom,
                    aoi_properties=aoi_feature.properties,
                    params=validated_params,
                )
                artifact_errors = resources.artifact_errors
                self.storage.save_all(artifacts, file_dir=resources.computation_dir)

            computation_info = self.backend_db.read_computation(correlation_uuid=correlation_uuid)
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


def run_standalone(
    operator: BaseOperator,
    computation_id: UUID,
    aoi_geom: shapely.MultiPolygon,
    aoi_properties: AoiProperties,
    params: BaseModel,
    lang: str,
    output_dir: Path,
) -> StandAloneComputationInfo:
    resources = ComputationResources(correlation_uuid=computation_id, computation_dir=output_dir)

    validated_params = operator.validate_params(params=params.model_dump())

    set_language(lang=lang, localisation_dir=operator.info_enriched.assets.localisation_directory)

    artifacts = operator.compute_unsafe(
        resources=resources, aoi=aoi_geom, aoi_properties=aoi_properties, params=validated_params
    )
    render_charts(artifacts=artifacts, file_dir=resources.computation_dir, output_dir=output_dir)
    write_individual_artifact_metadata(artifacts=artifacts, output_dir=output_dir)

    aoi_feature = AoiFeatureModel(type='Feature', geometry=aoi_geom, properties=aoi_properties)
    plugin_info = ComputationPluginInfo(
        id=operator.info_enriched.id, version=operator.info_enriched.version, language=lang
    )
    computation_info = StandAloneComputationInfo(
        correlation_uuid=computation_id,
        request_ts=datetime.now(),
        deduplication_key=uuid.uuid4(),
        cache_epoch=None,
        language=lang,
        valid_until=datetime.now(),
        params=validated_params.model_dump(mode='json'),
        requested_params=params.model_dump(mode='json'),
        aoi=aoi_feature,
        plugin_info=plugin_info,
        artifacts=artifacts,
        artifact_errors=resources.artifact_errors,
        output_dir=output_dir,
    )

    with open(output_dir / COMPUTATION_INFO_FILENAME, 'x') as out_file:
        out_file.write(computation_info.model_dump_json(indent=4))

    return computation_info


def render_charts(artifacts: list[ArtifactEnriched], file_dir: Path, output_dir: Path) -> None:
    rendered_artifacts = []
    for artifact in artifacts:
        if artifact.modality == ArtifactModality.CHART_PLOTLY:
            fig = plotly.io.read_json(file_dir / artifact.filename)
            fig.write_html(output_dir / f'{artifact.filename}_rendered.html')
            rendered_artifacts.append(artifact.name)
    log.debug(f'Rendered {rendered_artifacts}')


def write_individual_artifact_metadata(artifacts: list[ArtifactEnriched], output_dir: Path) -> None:
    for artifact in artifacts:
        with open(output_dir / f'{artifact.filename}.{COMPUTATION_INFO_FILENAME}', 'x') as artifact_metadata_file:
            artifact_metadata_file.write(artifact.model_dump_json(indent=4))


@task_revoked.connect
def uncache_revoked_task_on_worker(**kwargs):
    correlation_uuid = kwargs['request'].id
    sender = kwargs['sender']
    sender.backend_db.update_failed_computation(correlation_uuid=correlation_uuid, failure_message=None, cache=False)
