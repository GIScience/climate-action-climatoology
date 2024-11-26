import logging
import uuid
from abc import ABC, abstractmethod
from typing import Set
from uuid import UUID

import geojson_pydantic
from celery import Celery
from celery.exceptions import TimeoutError
from celery.result import AsyncResult
from semver import Version

import climatoology
from climatoology.app.plugin import generate_plugin_name
from climatoology.app.settings import CABaseSettings, SenderSettings
from climatoology.base.baseoperator import AoiProperties
from climatoology.base.info import _Info
from climatoology.utility.exception import InfoNotReceivedException, ClimatoologyVersionMismatchException

log = logging.getLogger(__name__)


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

        self.celery_app = CeleryPlatform.construct_celery_app(sender_config)
        self.assert_plugin_version = sender_config.assert_plugin_version

    @staticmethod
    def construct_celery_app(sender_config: SenderSettings) -> Celery:
        settings = CABaseSettings()
        celery_app = Celery(
            'sender',
            broker=settings.broker_connection_string,
            backend=settings.backend_connection_string,
        )

        celery_app.conf.update(**sender_config.model_dump(exclude={'assert_plugin_version'}))
        return celery_app

    def list_active_plugins(self) -> Set[str]:
        """Retrieve a list of active plugins.

        :return: List of plugin ids
        """
        available_tasks = self.celery_app.control.inspect().registered() or dict()
        plugins = {CeleryPlatform._extract_plugin_id(k) for k, v in available_tasks.items() if v == ['compute', 'info']}

        log.debug(f'Active plugins: {plugins}.')

        return plugins

    def request_info(self, plugin_id: str, ttl: int = 3) -> _Info:
        log.debug(f"Requesting 'info' from {plugin_id}.")
        correlation_uuid = str(uuid.uuid4())

        plugin_name = generate_plugin_name(plugin_id)
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

        if self.assert_plugin_version and not Version.parse(info_return.library_version).is_compatible(
            climatoology.__version__
        ):
            raise ClimatoologyVersionMismatchException(
                f'Refusing to register plugin '
                f'{info_return.name} in version {info_return.version} due to a library '
                f'version mismatch. '
                f'Local library version: {climatoology.__version__} <-> '
                f'Plugin library version: {info_return.library_version}'
            )

        return info_return

    def send_compute_request(
        self,
        plugin_id: str,
        aoi: geojson_pydantic.Feature[geojson_pydantic.MultiPolygon, AoiProperties],
        params: dict,
        correlation_uuid: UUID,
    ) -> AsyncResult:
        assert aoi.properties is not None, 'AOI properties are required'

        plugin_name = generate_plugin_name(plugin_id)

        return self.celery_app.send_task(
            name='compute',
            kwargs={
                'aoi': aoi.geometry.model_dump(mode='json'),
                'aoi_properties': aoi.properties.model_dump(mode='json'),
                'params': params,
            },
            task_id=str(correlation_uuid),
            routing_key=plugin_name,
            exchange='C.dq2',
        )

    @staticmethod
    def _extract_plugin_id(plugin_id_with_suffix: str) -> str:
        return plugin_id_with_suffix.split('@')[0]
