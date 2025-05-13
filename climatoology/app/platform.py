from enum import StrEnum
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Optional, Set
from uuid import UUID

import geojson_pydantic
from celery import Celery
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from semver import Version
from typing_extensions import deprecated

import climatoology
from climatoology.app.plugin import generate_plugin_name
from climatoology.app.settings import CABaseSettings, SenderSettings
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.info import _Info
from climatoology.store.database.database import BackendDatabase
from climatoology.store.object_store import MinioStorage, Storage
from climatoology.utility.exception import InfoNotReceivedException, VersionMismatchException

log = logging.getLogger(__name__)


class CacheOverrides(StrEnum):
    FOREVER = 'forever-cache'
    NEVER = 'no-cache'


class Platform(ABC):
    """A platform class that holds connections and provides access to an underlying computation platform framework."""

    @abstractmethod
    def list_active_plugins(self) -> Set[str]:
        """Get a set of currently active plugins on the platform."""

    @abstractmethod
    def request_info(self, plugin_id: str) -> _Info:
        """Get an info object from a plugin.

        :param plugin_id: The plugin to inquire info from
        :return: Information on the plugin
        """
        pass

    @abstractmethod
    def send_compute_request(
        self,
        plugin_id: str,
        aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties],
        params: dict,
        correlation_uuid: UUID,
    ) -> AsyncResult:
        """Trigger a computation.

        :param plugin_id: The target plugin
        :param aoi: Area of interest
        :param params: The computation configuration parameters
        :param correlation_uuid: The computations' correlation uuid
        """
        pass


class CeleryPlatform(Platform):
    def __init__(self):
        sender_config = SenderSettings()

        # we require the user to set the settings via a .env file or via env variables
        # noinspection PyArgumentList
        settings = CABaseSettings()

        self.celery_app = CeleryPlatform.construct_celery_app(settings, sender_config)

        self.assert_plugin_version = sender_config.assert_plugin_version

        self.storage = CeleryPlatform.construct_storage(settings)

        self.backend_db = BackendDatabase(
            connection_string=settings.db_connection_string, user_agent=f'CeleryPlatform/{climatoology.__version__}'
        )
        self.deduplicate_computations = settings.deduplicate_computations

    @staticmethod
    def construct_celery_app(settings: CABaseSettings, sender_config: SenderSettings) -> Celery:
        celery_app = Celery(
            'sender',
            broker=settings.broker_connection_string,
            backend=settings.backend_connection_string,
        )

        celery_app.conf.update(**sender_config.model_dump(exclude={'assert_plugin_version'}))
        return celery_app

    @staticmethod
    def construct_storage(settings: CABaseSettings) -> Storage:
        return MinioStorage(
            host=settings.minio_host,
            port=settings.minio_port,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            bucket=settings.minio_bucket,
            secure=settings.minio_secure,
        )

    def list_active_plugins(self) -> Set[str]:
        """Retrieve a list of active plugins.

        :return: List of plugin ids
        """
        available_tasks = self.celery_app.control.inspect().registered() or dict()
        plugins = {CeleryPlatform._extract_plugin_id(k) for k, v in available_tasks.items() if 'compute' in v}

        log.debug(f'Active plugins: {plugins}.')

        return plugins

    def request_info(self, plugin_id: str, ttl: int = 3) -> _Info:
        log.debug(f"Requesting 'info' from {plugin_id}.")
        correlation_uuid = str(uuid.uuid4())

        plugin_name = generate_plugin_name(plugin_id)
        try:
            info_return = self.backend_db.read_info(plugin_id=plugin_id)
        except InfoNotReceivedException:
            info_return = self.get_info_via_task(correlation_uuid, plugin_name, ttl)
            log.warning(
                f'Plugin {plugin_id} is running on an old version of climatoology that will no longer be supported. Please update!'
            )

        if self.assert_plugin_version and not Version.parse(info_return.library_version).is_compatible(
            climatoology.__version__
        ):
            raise VersionMismatchException(
                f'Refusing to register plugin '
                f'{info_return.name} in version {info_return.version} due to a climatoology library '
                f'version mismatch. '
                f'Local library version: {climatoology.__version__} <-> '
                f'Plugin library version: {info_return.library_version}'
            )

        return info_return

    @deprecated('This method is kept for backwards compatibility until the next major release.')
    def get_info_via_task(self, correlation_uuid, plugin_name, ttl):
        self.celery_app.send_task(
            'info', task_id=correlation_uuid, routing_key=plugin_name, exchange='C.dq2', expires=ttl
        )
        result = self.celery_app.AsyncResult(correlation_uuid)
        try:
            raw_info = result.get(timeout=ttl)
        except TimeoutError as e:
            raise InfoNotReceivedException(
                f'The info request ({correlation_uuid}) did not respond within the time limit of {ttl} seconds.'
            ) from e
        info_return = _Info(**raw_info)
        return info_return

    def send_compute_request(
        self,
        plugin_id: str,
        aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties],
        params: dict,
        correlation_uuid: UUID,
        override_shelf_life: Optional[CacheOverrides] = None,
        task_time_limit: float = None,
        q_time: float = None,
    ) -> AsyncResult:
        assert aoi.properties is not None, 'AOI properties are required'

        plugin_info = self.request_info(plugin_id)

        match override_shelf_life:
            case CacheOverrides.FOREVER:
                computation_shelf_life = None
            case CacheOverrides.NEVER:
                computation_shelf_life = timedelta(0)
            case _:
                computation_shelf_life = (
                    plugin_info.computation_shelf_life if self.deduplicate_computations else timedelta(0)
                )

        # Register the task now, before it gets queued
        deduplicated_correlation_uuid = self.backend_db.register_computation(
            plugin_id=plugin_id,
            plugin_version=plugin_info.version,
            computation_shelf_life=computation_shelf_life,
            correlation_uuid=correlation_uuid,
            requested_params=params,
            aoi=aoi,
        )

        if deduplicated_correlation_uuid == correlation_uuid:
            plugin_name = generate_plugin_name(plugin_id)
            return self.celery_app.send_task(
                name='compute',
                kwargs={
                    'aoi': aoi.model_dump(mode='json'),
                    'params': params,
                },
                task_id=str(correlation_uuid),
                routing_key=plugin_name,
                exchange='C.dq2',
                time_limit=task_time_limit,
                expires=q_time,
            )
        else:
            log.info(
                f'Computation request {correlation_uuid} is deduplicated with computation {deduplicated_correlation_uuid}'
            )
            return AsyncResult(id=str(deduplicated_correlation_uuid), app=self.celery_app)

    @staticmethod
    def _extract_plugin_id(plugin_id_with_suffix: str) -> str:
        return plugin_id_with_suffix.split('@')[0]
