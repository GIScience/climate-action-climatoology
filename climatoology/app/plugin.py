import asyncio
import logging
import time
from typing import Optional

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from climatoology.base.artifact import Artifact
from climatoology.base.computation import ComputationScope
from climatoology.base.event import ComputeCommand, ComputeCommandStatus, InfoCommand
from climatoology.base.operator import Operator
from climatoology.broker.message_broker import AsyncRabbitMQ
from climatoology.store.object_store import Storage

log = logging.getLogger(__name__)


class PlatformPlugin:
    """Climate Action Platform worker.

    It's responsible for handling user commands and emitting system-wide notifications.
    The main plugin logic and workload is handled by the Operator.
    """

    def __init__(self,
                 operator: Operator,
                 storage: Storage,
                 broker: AsyncRabbitMQ):
        self.operator = operator
        self.storage = storage
        self.broker = broker

        name = operator.info().name

        self.compute_queue_name = broker.get_compute_queue(plugin_name=name)
        self.info_queue_name = broker.get_info_queue(plugin_name=name)
        self.compute_queue: Optional[aio_pika.Queue] = None
        self.info_queue: Optional[aio_pika.Queue] = None

    async def __compute_callback(self, message: AbstractIncomingMessage):
        command = ComputeCommand.model_validate_json(message.body)
        log.debug(f'Acquired compute request ({command.correlation_uuid})')

        await self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                                status=ComputeCommandStatus.IN_PROGRESS)

        try:
            tic = time.perf_counter()

            with ComputationScope(command.correlation_uuid) as resources:
                artifacts = self.operator.compute_unsafe(resources, command.params)
                plugin_artifacts = [Artifact(correlation_uuid=command.correlation_uuid,
                                             **artifact.model_dump(exclude={'correlation_uuid'}))
                                    for artifact in artifacts]
                self.storage.save_all(plugin_artifacts)

            toc = time.perf_counter()

            await self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                                    status=ComputeCommandStatus.COMPLETED,
                                                    message=f'Took {toc - tic:0.4f} seconds')
        except (ValueError, ValidationError) as e:
            await self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                                    status=ComputeCommandStatus.FAILED,
                                                    message=str(e))

        await message.ack()

    async def __info_callback(self, message):
        command: InfoCommand = InfoCommand.model_validate_json(message.body)
        log.debug(f'Acquired info request ({command.correlation_uuid})')

        out_body = self.operator.info_enriched().model_dump_json().encode()

        async with self.broker.channel_pool.acquire() as channel:
            await channel.default_exchange.publish(message=aio_pika.Message(body=out_body),
                                                   routing_key=message.properties.reply_to)
            await message.ack()

    async def run(self) -> None:
        log.debug('Running plugin loop')

        async with self.broker.channel_pool.acquire() as channel:
            await channel.set_qos(prefetch_count=1)

            compute_queue = await channel.declare_queue(name=self.compute_queue_name, durable=True)
            info_queue = await channel.declare_queue(name=self.info_queue_name)

            await self.broker.loop.create_task(compute_queue.consume(self.__compute_callback))
            await self.broker.loop.create_task(info_queue.consume(self.__info_callback))
            await asyncio.Future()
