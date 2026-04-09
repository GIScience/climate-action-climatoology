import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List
from unittest.mock import ANY, patch

import pytest
import shapely
from celery import Celery
from pydantic_extra_types.language_code import LanguageAlpha2
from semver import Version
from sqlalchemy import select
from sqlalchemy.orm import Session

from climatoology.app.exception import VersionMismatchError
from climatoology.app.plugin import (
    _create_plugin,
    _version_is_compatible,
    extract_plugin_id,
    run_standalone_computation,
    synch_info,
)
from climatoology.base.artifact import Artifact, ArtifactEnriched, Chart2dData, ChartType
from climatoology.base.artifact_creators import create_chart_artifact
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import (
    AoiProperties,
    ComputationInfo,
    ComputationResources,
    StandAloneComputationInfo,
)
from climatoology.base.exception import InputValidationError
from climatoology.base.i18n import N_
from climatoology.base.plugin_info import PluginInfo
from climatoology.store.database.models.plugin_info import PluginInfoTable
from test.fixtures.plugin_info import TestModel


def test_run_standalone_computation(
    default_operator,
    default_input_model,
    default_computation_info,
    default_aoi_geom_shapely,
    default_aoi_properties,
    general_uuid,
    frozen_time,
):
    base_computation_info = default_computation_info.model_copy(deep=True)
    with (
        tempfile.TemporaryDirectory() as result_dir_str,
        patch('climatoology.app.tasks.uuid.uuid4', return_value=general_uuid),
    ):
        result_dir = Path(result_dir_str) / 'test-results'

        expected_computation_info = StandAloneComputationInfo(
            **base_computation_info.model_dump(), output_dir=result_dir
        )
        expected_computation_info.requested_params = ANY
        expected_computation_info.deduplication_key = ANY
        expected_computation_info.cache_epoch = None
        expected_computation_info.valid_until = datetime.now()

        computation_info = run_standalone_computation(
            operator=default_operator,
            output_dir=result_dir,
            aoi_geom=default_aoi_geom_shapely,
            aoi_properties=default_aoi_properties,
            params=default_input_model,
        )

        assert computation_info == expected_computation_info

        result = result_dir / 'test_artifact_file.md'
        assert result.is_file()
        assert result.read_text() == 'English Text'

        artifact_metadata = result_dir / 'test_artifact_file.md.metadata.json'
        assert artifact_metadata.is_file()
        assert ArtifactEnriched.model_validate_json(artifact_metadata.read_text())

        metadata = result_dir / 'metadata.json'
        assert metadata.is_file()
        assert ComputationInfo.model_validate_json(metadata.read_text())


def test_run_standalone_computation_renders_charts(default_plugin_info, default_input_model, default_artifact_metadata):
    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> PluginInfo:
            return default_plugin_info.model_copy(deep=True)

        def compute(
            self,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
        ) -> List[Artifact]:
            assert isinstance(aoi, shapely.MultiPolygon)
            chart = Chart2dData(x=[1, 2], y=[3, 4], chart_type=ChartType.LINE)
            artifact = create_chart_artifact(data=chart, metadata=default_artifact_metadata, resources=resources)
            return [artifact]

    with tempfile.TemporaryDirectory() as result_dir_str:
        result_dir = Path(result_dir_str) / 'test-results'

        geom = shapely.MultiPolygon()
        properties = AoiProperties(name='Standalone Computation', id='aa')

        _ = run_standalone_computation(
            operator=TestOperator(),
            output_dir=result_dir,
            aoi_geom=geom,
            aoi_properties=properties,
            params=default_input_model,
        )

        result = result_dir / 'test_artifact_file.json'
        assert result.is_file()

        result = result_dir / 'test_artifact_file.json_rendered.html'
        assert result.is_file()


def test_plugin_creation(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)
        assert isinstance(plugin, Celery)


def test_plugin_register_task(default_operator, default_settings, mocked_object_store, default_backend_db):
    with patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        assert plugin.tasks.unregister('compute') is None


def test_worker_send_compute_task(
    default_plugin,
    default_computation_info,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True).model_dump(mode='json')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }

    computed_compute_result = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    assert computed_compute_result == expected_computation_info


def test_successful_compute_saves_metadata_to_storage(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    default_computation_info,
    mocker,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)
    save_info_spy = mocker.spy(default_plugin.tasks['compute'], '_save_computation_info')

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }
    _ = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    save_info_spy.assert_called_once_with(computation_info=expected_computation_info)


def test_successful_compute_saves_metadata_to_backend(
    default_plugin,
    general_uuid,
    default_aoi_feature_pure_dict,
    backend_with_computation_registered,
    default_computation_info,
    frozen_time,
):
    expected_computation_info = default_computation_info.model_copy(deep=True)
    expected_computation_info.artifacts[0].rank = 0

    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {'id': 1},
    }
    _ = default_plugin.send_task(
        'compute',
        kwargs=kwargs,
        task_id=str(general_uuid),
    ).get(timeout=5)

    saved_computation = backend_with_computation_registered.read_computation(correlation_uuid=general_uuid)
    assert saved_computation == expected_computation_info


def test_failing_compute_updates_backend(
    default_plugin, general_uuid, default_aoi_feature_pure_dict, backend_with_computation_registered
):
    kwargs = {
        'aoi': default_aoi_feature_pure_dict,
        'params': {},
    }
    with pytest.raises(InputValidationError, match='ID: Field required. You provided: {}.'):
        _ = default_plugin.send_task(
            'compute',
            kwargs=kwargs,
            task_id=str(general_uuid),
        ).get(timeout=5)

    updated_computation = backend_with_computation_registered.read_computation(correlation_uuid=general_uuid)
    assert updated_computation.message == 'ID: Field required. You provided: {}.'


def test_version_matches_raises_on_lower(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    older_plugin_info = default_plugin_info_final
    older_plugin_info.version = Version(2, 1, 0)
    with pytest.raises(VersionMismatchError, match=r'Refusing to register plugin*'):
        _version_is_compatible(info=older_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_equal(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)
    assert _version_is_compatible(info=default_plugin_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_no_plugin_registered(default_backend_db, default_plugin_info_final, celery_app):
    assert _version_is_compatible(info=default_plugin_info_final, db=default_backend_db, celery=celery_app)


def test_version_matches_higher(default_backend_db, default_plugin_info_final, celery_app):
    _ = default_backend_db.write_info(info=default_plugin_info_final)

    newer_plugin_info = default_plugin_info_final
    newer_plugin_info.version = Version(3, 1, 1)
    assert _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_version_matches_higher_not_alone(default_plugin, default_backend_db, default_plugin_info_final, celery_app):
    newer_plugin_info = default_plugin_info_final.model_copy(deep=True)
    newer_plugin_info.version = Version(3, 1, 1)
    with pytest.raises(
        AssertionError,
        match=re.escape(
            'Refusing to register plugin Test Plugin version 3.1.1 because a plugin with a lower version (3.1.0) is already running. Make sure to stop it before upgrading.',
        ),
    ):
        _version_is_compatible(info=newer_plugin_info, db=default_backend_db, celery=celery_app)


def test_extract_plugin_id():
    computed_plugin_id = extract_plugin_id('a@b')
    assert computed_plugin_id == 'a'


def test_synch_info_multiple_languages(default_backend_db, default_plugin_info_enriched, mocked_object_store):
    plugin_info_enriched = default_plugin_info_enriched.model_copy(deep=True)

    # Mark an Enum value for translation to ensure it is NOT translated.
    # Enums cannot be translated because the plugin requires the exact value to be provided, so it can instantiate the
    # Enum from the value.
    # In the future, we could manually add a translation mapping to the `$defs` of the dumped operator schema.
    opt1 = N_('OPT1')

    synched_info = synch_info(info=plugin_info_enriched, db=default_backend_db, storage=mocked_object_store)

    assert synched_info.keys() == {'de', 'en'}
    assert synched_info[LanguageAlpha2('de')].teaser == 'Deutscher Klappentext mit etwas Länge.'

    # This is the only place that we assert the json schema
    assert synched_info[LanguageAlpha2('de')].operator_schema == {
        '$defs': {
            'Option': {'enum': [opt1, 'OPT2'], 'title': 'Option', 'type': 'string'},
            'Mapping': {
                'properties': {
                    'key': {
                        'default': 'value',
                        'title': 'Key',
                        'type': 'string',
                    },
                },
                'title': 'Mapping',
                'type': 'object',
            },
        },
        'properties': {
            'execution_time': {
                'default': 0.0,
                'description': 'Wie lange die Berechnung dauern soll (in Sekunden)',
                'examples': [10.0],
                'title': 'Ausführungsdauer',
                'type': 'number',
            },
            'id': {
                'description': 'Eine verpflichtende Ganzzahlangabe.',
                'examples': [1],
                'title': 'Identifikationsnummer',
                'type': 'integer',
            },
            'mapping': {
                '$ref': '#/$defs/Mapping',
                'default': {
                    'key': 'value',
                },
            },
            'name': {
                'default': 'John Doe',
                'description': 'Ein optionaler Parameter.',
                'examples': ['John Doe'],
                'title': 'Der Name',
                'type': 'string',
            },
            'option': {
                '$ref': '#/$defs/Option',
                'default': 'OPT1',
            },
        },
        'required': ['id'],
        'title': 'TestModel',
        'type': 'object',
    }

    with Session(default_backend_db.engine) as session:
        select_stmt = select(PluginInfoTable.language, PluginInfoTable.latest)
        infos = session.execute(select_stmt).all()

    assert infos == [('en', True), ('de', True)]
