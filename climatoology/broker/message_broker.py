import asyncio
import json
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional
from uuid import UUID

import aio_pika
import requests
from aio_pika.abc import ExchangeType
from aio_pika.pool import Pool
from semver import Version

import climatoology
from climatoology.base.event import InfoCommand, ComputeCommand, ComputeCommandStatus, ComputeCommandResult
from climatoology.base.operator import Info
from climatoology.utility.exception import InfoNotReceivedException, ClimatoologyVersionMismatchException

log = logging.getLogger(__name__)

QUEUE_SEPARATOR = '_'
STATUS_EXCHANGE = 'notify'
INFO_QUEUE: str = 'info'
COMPUTE_QUEUE: str = 'compute'


class Broker(ABC):
    """A message broker class that holds connections and provides access to an underlying message broker framework."""

    @staticmethod
    def get_status_exchange() -> str:
        """Retrieve the status exchange channel name.

        :return: Status exchange channel name
        """
        return STATUS_EXCHANGE

    @staticmethod
    def get_compute_queue(plugin_id: str) -> str:
        """Get the compute queue name for a specific plugin.

        :param plugin_id: The plugin id
        :return: The compute queue name for that plugin
        """
        return f'{plugin_id}{QUEUE_SEPARATOR}{COMPUTE_QUEUE}'

    @staticmethod
    def get_info_queue(plugin_id: str) -> str:
        """Get the info queue name for a specific plugin.

        :param plugin_id: The plugin id
        :return: The info queue name for that plugin
        """
        return f'{plugin_id}{QUEUE_SEPARATOR}{INFO_QUEUE}'

    @abstractmethod
    def publish_status_update(
        self,
        correlation_uuid: UUID,
        status: ComputeCommandStatus,
        message: str = None,
    ) -> None:
        """Push a compute status update to the broker.

        :param correlation_uuid: The correlation uuid of the computation
        :param status: The new status
        :param message: An optional message to be added to the status
        """
        pass

    @abstractmethod
    def request_info(self, plugin_id: str) -> Info:
        """Get an info object from a plugin.

        :param plugin_id: The plugin to inquire info from
        :return: Information on the plugin
        """
        pass

    @abstractmethod
    def send_compute(self, plugin_id: str, params: dict, correlation_uuid: UUID) -> None:
        """Trigger a computation.

        :param plugin_id: The target plugin
        :param params: The computation configuration parameters
        :param correlation_uuid: The computations' correlation uuid
        """
        pass


class AsyncRabbitMQ(Broker):
    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        connection_pool_max_size: int = 2,
        assert_plugin_version: bool = True,
    ):
        """Initialise a AsyncRabbitMQ-broker wrapper.

        :param host: The host url
        :param port: The port
        :param user: The brokers' authentication user
        :param password: The brokers' authentication password
        :param connection_pool_max_size: The maximum number of connections in the pool
        :param assert_plugin_version: Should the info request only be accepted for plugins that use a compatible
        climatoology library version? If True, the info request may throw a ClimatoologyVersionMismatchException. This
        flag can prevent outdated plugins from being announced on the platform, as they may create incompatible data.
        """
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.connection_pool_max_size = connection_pool_max_size
        self.loop = asyncio.get_event_loop()
        self.connection_pool: Optional[Pool] = None
        self.assert_plugin_version = assert_plugin_version

    async def async_init(self):
        async def get_connection():
            return await aio_pika.connect(host=self.host, port=self.port, login=self.user, password=self.password)

        self.connection_pool: Pool = Pool(get_connection, max_size=self.connection_pool_max_size, loop=self.loop)

    def __await__(self):
        return self.async_init().__await__()

    async def publish_status_update(
        self, correlation_uuid: UUID, status: ComputeCommandStatus, message: str = None
    ) -> None:
        log.debug(f'Sending compute update for {correlation_uuid}: {status.name} - {message}.')
        compute_command = ComputeCommandResult(
            correlation_uuid=correlation_uuid,
            status=status,
            message=message,
            timestamp=datetime.now(),
        )
        body = compute_command.model_dump_json().encode()
        async with self.connection_pool.acquire() as connection:
            async with connection.channel() as channel:
                exchange = await channel.declare_exchange(self.get_status_exchange(), type=ExchangeType.FANOUT)
                await exchange.publish(routing_key='', message=aio_pika.Message(body=body))

    async def request_info(self, plugin_id: str, ttl: int = 3) -> Info:
        log.debug(f"Requesting 'info' from {plugin_id}.")
        info_call_corr_uuid = uuid.uuid4()
        async with self.connection_pool.acquire() as connection:
            async with connection.channel() as channel:
                await channel.set_qos(prefetch_count=1)

                callback_queue = await channel.declare_queue(exclusive=True, auto_delete=True)

                info_command_body = InfoCommand(correlation_uuid=info_call_corr_uuid)
                await channel.default_exchange.publish(
                    message=aio_pika.Message(
                        body=info_command_body.model_dump_json().encode(), reply_to=callback_queue.name
                    ),
                    routing_key=self.get_info_queue(plugin_id=plugin_id),
                )

                try:
                    async with callback_queue.iterator(timeout=ttl) as queue_iter:
                        async for message in queue_iter:
                            async with message.process():
                                response = json.loads(message.body)
                                info_return = Info(**response)
                                if self.assert_plugin_version and not Version.parse(
                                    info_return.library_version
                                ).is_compatible(climatoology.__version__):
                                    raise ClimatoologyVersionMismatchException(
                                        f'Refusing to register plugin '
                                        f'{info_return.name} for library '
                                        f'version mismatch. '
                                        f'Local: {climatoology.__version__}, '
                                        f'Plugin: {info_return.version}'
                                    )
                                return info_return
                except TimeoutError as e:
                    raise InfoNotReceivedException(
                        f'The info request ({info_call_corr_uuid}) did not respond within the time '
                        f'limit of {ttl} seconds.'
                    ) from e

    async def send_compute(self, plugin_id: str, params: dict, correlation_uuid: UUID):
        log.debug(f"Requesting 'compute' from {plugin_id} under {correlation_uuid}.")
        async with self.connection_pool.acquire() as connection:
            async with connection.channel() as channel:
                await channel.declare_queue(self.get_compute_queue(plugin_id), durable=True)

                command = ComputeCommand(correlation_uuid=correlation_uuid, params=params)
                await channel.default_exchange.publish(
                    aio_pika.Message(body=command.model_dump_json().encode()),
                    routing_key=self.get_compute_queue(plugin_id),
                )


class RabbitMQManagementAPI:
    """An interface class for the RabbitMQ management API."""

    def __init__(self, api_url: str, user: str, password: str):
        self.api_url = api_url
        self.user = user
        self.password = password
        # Test connection
        _ = self.get_active_plugins()

    def get_active_plugins(self) -> List[str]:
        """Retrieve a list of active plugins.

        :return: List of plugin ids
        """
        url = f'{self.api_url}/api/queues'

        response = requests.get(url, auth=(self.user, self.password))
        response.raise_for_status()

        suffix = f'{QUEUE_SEPARATOR}{INFO_QUEUE}'
        plugins = [x['name'].removesuffix(suffix) for x in response.json() if suffix in x['name']]
        log.debug(f'Active plugins: {plugins}.')
        return plugins
