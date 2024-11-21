from typing import Optional

from celery import Celery

from climatoology.app.settings import CABaseSettings, WorkerSettings, CELERY_HOST_PLACEHOLDER
from climatoology.app.tasks import CAPlatformComputeTask, CAPlatformInfoTask
from climatoology.base.baseoperator import BaseOperator
from climatoology.store.object_store import MinioStorage


def start_plugin(operator: BaseOperator) -> Optional[int]:
    """Start a CA Plugin

    :param operator: The Operator that fills the plugin with life
    :return: This method does not return until the plugin is stopped. Then it will return the exit code
    """
    settings = CABaseSettings()
    plugin = _create_plugin(operator, settings)

    worker_config = WorkerSettings()
    plugin.conf.update(**worker_config.model_dump())

    plugin_name = generate_plugin_name(plugin_id=plugin.main)
    return plugin.start(['worker', '-n', plugin_name, '--loglevel', settings.log_level])


def _create_plugin(operator: BaseOperator, settings: CABaseSettings) -> Celery:
    plugin = Celery(
        operator.info_enriched.plugin_id,
        broker=settings.broker_connection_string,
        backend=settings.backend_connection_string,
    )

    storage = MinioStorage(
        host=settings.minio_host,
        port=settings.minio_port,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        bucket=settings.minio_bucket,
        secure=settings.minio_secure,
    )

    compute_task = CAPlatformComputeTask(operator, storage)
    plugin.register_task(compute_task)

    info_task = CAPlatformInfoTask(operator=operator, storage=storage, overwrite_assets=settings.overwrite_assets)
    plugin.register_task(info_task)

    return plugin


def generate_plugin_name(plugin_id: str) -> str:
    return f'{plugin_id}@{CELERY_HOST_PLACEHOLDER}'
