import time
from typing import Generator, List
from unittest.mock import Mock, patch

import pytest
import shapely
from celery import Celery, signals
from celery.utils.threads import LocalStack
from pydantic_extra_types.language_code import LanguageAlpha2

from climatoology.app.plugin import _create_plugin
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.artifact import Artifact
from climatoology.base.artifact_creators import create_markdown_artifact
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.computation import AoiProperties, ComputationResources
from climatoology.base.i18n import tr
from climatoology.base.plugin_info import PluginInfo
from test.conftest import TestModel, connection_to_string


@pytest.fixture
def default_operator(default_plugin_info, default_artifact_metadata) -> BaseOperator:
    class TestOperator(BaseOperator[TestModel]):
        def info(self) -> PluginInfo:
            return default_plugin_info.model_copy(deep=True)

        def compute(
            self,
            *,
            resources: ComputationResources,
            aoi: shapely.MultiPolygon,
            aoi_properties: AoiProperties,
            params: TestModel,
            language: LanguageAlpha2,
            **kwargs,
        ) -> List[Artifact]:
            # asset that the usage of this fixture is realistic, i.e. uses correct types
            assert isinstance(resources, ComputationResources)
            assert isinstance(aoi, shapely.MultiPolygon)
            assert isinstance(aoi_properties, AoiProperties)
            assert isinstance(params, TestModel)
            assert isinstance(language, LanguageAlpha2)
            assert kwargs == dict()

            time.sleep(params.execution_time)
            artifact_text = tr('placeholder_text')
            artifact = create_markdown_artifact(
                text=artifact_text,
                metadata=default_artifact_metadata,
                resources=resources,
            )
            return [artifact]

    return TestOperator()


@pytest.fixture
def default_computation_task(
    default_operator, mocked_object_store, default_backend_db, general_uuid
) -> CAPlatformComputeTask:
    compute_task = CAPlatformComputeTask(
        operator=default_operator, storage=mocked_object_store, backend_db=default_backend_db
    )
    compute_task.update_state = Mock()
    request = Mock()
    request.correlation_id = str(general_uuid)
    compute_task.request_stack = LocalStack()
    compute_task.request_stack.push(request)
    return compute_task


@pytest.fixture
def default_computation_task_de(
    default_operator, mocked_object_store, default_backend_db, general_uuid_de
) -> CAPlatformComputeTask:
    compute_task = CAPlatformComputeTask(
        operator=default_operator, storage=mocked_object_store, backend_db=default_backend_db
    )
    compute_task.update_state = Mock()
    request = Mock()
    request.correlation_id = str(general_uuid_de)
    compute_task.request_stack = LocalStack()
    compute_task.request_stack.push(request)
    return compute_task


@pytest.fixture()
def celery_config(celery_config, db_fixture_with_tables):
    connection_str = connection_to_string(db_fixture_with_tables)
    celery_config.update({'result_backend': f'db+{connection_str}'})
    return celery_config


@pytest.fixture
def celery_worker_parameters():
    return {'hostname': 'test_plugin@hostname'}


@signals.setup_logging.connect
def setup_celery_logging(**kwargs):
    """
    Override the default celery logging setup, which would otherwise override the root logger. By overriding the root
    logger, the pytest `caplog` fixture no longer works, because the `caplog` handler gets removed. For more info, see
    the warning here: https://docs.pytest.org/en/latest/how-to/logging.html#caplog-fixture

    Note that celery has a setting `worker_hijack_root_logger`, but this is somewhat misleading. Even if you set this to
    `False`, celery still interferes with the root logger.  This is intentional
    behaviour: https://github.com/celery/celery/pull/2016
    """
    pass


@pytest.fixture
def default_plugin(
    celery_app, celery_worker, default_operator, default_settings, mocked_object_store, default_backend_db
) -> Generator[Celery, None, None]:
    with (
        patch('climatoology.app.plugin.Celery', return_value=celery_app),
        patch('climatoology.app.plugin.BackendDatabase', return_value=default_backend_db),
    ):
        plugin = _create_plugin(operator=default_operator, settings=default_settings)

        celery_worker.reload()
        yield plugin
