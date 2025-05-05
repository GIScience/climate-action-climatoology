from typing import Optional

from celery import Celery

import climatoology
from climatoology.app.settings import CELERY_HOST_PLACEHOLDER, CABaseSettings, WorkerSettings
from climatoology.app.tasks import CAPlatformComputeTask
from climatoology.base.baseoperator import BaseOperator
from climatoology.base.info import _Info
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import MinioStorage, Storage


def start_plugin(operator: BaseOperator) -> Optional[int]:
    """Start a CA Plugin

    :param operator: The Operator that fills the plugin with life
    :return: This method does not return until the plugin is stopped. Then it will return the exit code
    """
    # we require the user to set the settings via a .env file or via env variables
    # noinspection PyArgumentList
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

    backend_database = BackendDatabase(
        connection_string=settings.db_connection_string,
        user_agent=f'Plugin {operator.info_enriched.plugin_id}/{operator.info_enriched.version} based on climatoology/{climatoology.__version__}',
    )

    _ = synch_info(
        info=operator.info_enriched, db=backend_database, storage=storage, overwrite=settings.overwrite_assets
    )

    compute_task = CAPlatformComputeTask(operator=operator, storage=storage, backend_db=backend_database)
    plugin.register_task(compute_task)

    return plugin


def generate_plugin_name(plugin_id: str) -> str:
    return f'{plugin_id}@{CELERY_HOST_PLACEHOLDER}'


def synch_info(info: _Info, db: BackendDatabase, storage: Storage, overwrite: bool) -> _Info:
    assets = storage.synch_assets(
        plugin_id=info.plugin_id,
        plugin_version=info.version,
        assets=info.assets,
        overwrite=overwrite,
    )
    info.assets = assets

    _ = db.write_info(info=info, revert=overwrite)

    return info
