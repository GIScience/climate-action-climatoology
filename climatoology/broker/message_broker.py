import json
import logging
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Optional
from uuid import UUID

from amqpstorm.management import ManagementApi
from cachetools import cached, TTLCache
from pika import ConnectionParameters, BasicProperties, BlockingConnection
from pika.adapters.blocking_connection import BlockingChannel
from pydantic import BaseModel

import climatoology
from climatoology.base.event import InfoCommand, ComputeCommand, ComputeCommandStatus, ComputeCommandResult
from climatoology.base.operator import Info
from climatoology.utility.exception import InfoNotReceivedException


class InfoCallbackHolder(BaseModel):
    correlation_uuid: UUID
    info_return: Optional[Info] = None

    def on_response(self, _ch, _method, props, body):
        if self.correlation_uuid == UUID(props.correlation_id):
            response = json.loads(body)
            self.info_return = Info.model_construct(**response)


class Broker(ABC):

    def __init__(self,
                 status_queue: str = 'notify',
                 info_suffix: str = 'info',
                 compute_suffix: str = 'compute'):
        self._status_queue = status_queue
        self._info_suffix = info_suffix
        self._compute_suffix = compute_suffix
        self._seperator = '_'

    def get_status_queue(self):
        return self._status_queue

    def get_compute_queue(self, plugin_name: str):
        return f'{plugin_name.lower()}{self._seperator}{self._compute_suffix}'

    def get_info_queue(self, plugin_name: str):
        return f'{plugin_name.lower()}{self._seperator}{self._info_suffix}'

    @abstractmethod
    def get_channel(self):
        pass

    @abstractmethod
    def publish_status_update(self, correlation_uuid: UUID, status: ComputeCommandStatus, message: str = None) -> None:
        pass

    @abstractmethod
    def request_info(self, plugin_name: str) -> Info:
        pass

    @abstractmethod
    def send_compute(self, plugin_name: str, params: dict, correlation_uuid: UUID):
        pass


class RabbitMQ(Broker):

    def __init__(self,
                 host: str,
                 port: int):
        super().__init__()
        self.amqp_connection = BlockingConnection(ConnectionParameters(host=host,
                                                                       port=port))

    def get_channel(self) -> BlockingChannel:
        return self.amqp_connection.channel()

    def publish_status_update(self,
                              correlation_uuid: UUID,
                              status: ComputeCommandStatus,
                              message: str = None) -> None:
        compute_command = ComputeCommandResult(correlation_uuid=correlation_uuid,
                                               status=status,
                                               message=message,
                                               timestamp=datetime.now())
        body = compute_command.model_dump_json().encode()
        self.get_channel().basic_publish(exchange=self.get_status_queue(), routing_key='', body=body)

    def request_info(self, plugin_name: str, ttl: int = 10) -> Info:
        info_call_corr_uuid = uuid.uuid4()
        channel = self.get_channel()
        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue

        info_command_body = InfoCommand(correlation_uuid=info_call_corr_uuid)

        callback_holder = InfoCallbackHolder(correlation_uuid=info_call_corr_uuid)
        channel.basic_consume(
            queue=callback_queue,
            on_message_callback=callback_holder.on_response,
            auto_ack=True)

        channel.basic_publish(exchange='',
                              routing_key=self.get_info_queue(plugin_name=plugin_name),
                              properties=BasicProperties(reply_to=callback_queue),
                              body=info_command_body.model_dump_json().encode())
        self.amqp_connection.process_data_events(time_limit=ttl)

        info_return = callback_holder.info_return

        if not info_return:
            raise InfoNotReceivedException(f'The info request ({info_call_corr_uuid}) did not respond within the time '
                                           f'limit of {ttl} seconds.')
        assert info_return.library_version == climatoology.__version__, \
            (f'Refusing to register plugin {plugin_name} for library version mismatch. '
             f'Local: {climatoology.__version__}, Plugin: {info_return.version}')

        return info_return

    def send_compute(self, plugin_name: str, params: dict, correlation_uuid: UUID) -> None:
        self.get_channel().queue_declare(queue=self.get_compute_queue(plugin_name), passive=True)
        command = ComputeCommand(correlation_uuid=correlation_uuid,
                                 params=params)
        self.get_channel().basic_publish(exchange='',
                                         routing_key=self.get_compute_queue(plugin_name),
                                         body=command.model_dump_json().encode())


class ManagedBroker(ABC):
    @abstractmethod
    def list_plugins(self) -> List[Info]:
        pass


class ManagedRabbitMQ(RabbitMQ, ManagedBroker):
    def __init__(self,
                 api_url: str,
                 user: str,
                 password: str,
                 host: str,
                 port: int):
        super().__init__(host, port)
        self.api_connection = ManagementApi(api_url=api_url,
                                            username=user,
                                            password=password)

    def _get_active_plugins(self) -> List[str]:
        plugins = []
        for queue in self.api_connection.queue.list(name=f'.*{self._seperator}{self._info_suffix}$', use_regex=True):
            plugins.append(queue['name'].removesuffix(f'{self._seperator}{self._info_suffix}'))
        return plugins

    @cached(cache=TTLCache(maxsize=1, ttl=60))
    def list_plugins(self) -> List[Info]:
        plugins = self._get_active_plugins()
        plugin_list = []
        for plugin in plugins:
            try:
                plugin_list.append(self.request_info(plugin))
            except InfoNotReceivedException as e:
                logging.warning(f'Plugin {plugin} has an open channel but could not be reached.',
                                exc_info=e)
                continue
            except AssertionError as e:
                logging.warning(f'Version mismatch for plugin {plugin}',
                                exc_info=e)
                continue

        return plugin_list
