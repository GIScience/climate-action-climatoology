import json
import time

from pika import BasicProperties
from pydantic import ValidationError

from climatoology.base.event import ComputeCommand, ComputeCommandStatus, InfoCommand
from climatoology.base.operator import Operator, Artifact
from climatoology.broker.message_broker import Broker
from climatoology.store.object_store import Storage


class PlatformPlugin:
    """Climate Action Platform worker.

    It's responsible for handling user commands and emitting system-wide notifications.
    The main plugin logic and workload is handled by the Operator.
    """

    def __init__(self,
                 operator: Operator,
                 storage: Storage,
                 broker: Broker):
        self.operator = operator
        self.storage = storage
        self.broker = broker

        name = operator.info().name.lower()

        self.compute_queue = broker.get_compute_queue(plugin_name=name)
        self.info_queue = broker.get_info_queue(plugin_name=name)
        self.notification_queue = broker.get_status_queue()

        self.channel = self.broker.get_channel()

        self.channel.basic_qos(prefetch_count=1)

        self.channel.queue_declare(queue=self.compute_queue, durable=True)
        self.channel.exchange_declare(self.notification_queue, exchange_type='fanout')
        self.channel.queue_declare(queue=self.info_queue)

    def __compute_callback(self, ch, method, _, body):
        command = ComputeCommand.model_validate_json(body)
        self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                          status=ComputeCommandStatus.IN_PROGRESS)

        try:
            tic = time.perf_counter()

            artifacts = self.operator.compute_unsafe(command.params)
            plugin_artifacts = [Artifact(correlation_uuid=command.correlation_uuid,
                                         params=command.params,
                                         **artifact.model_dump(exclude={'correlation_uuid', 'params'}))
                                for artifact in artifacts]
            self.storage.save_all(plugin_artifacts)

            toc = time.perf_counter()

            self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                              status=ComputeCommandStatus.COMPLETED,
                                              message=f'Took {toc - tic:0.4f} seconds')
        except ValueError | ValidationError as e:
            self.broker.publish_status_update(correlation_uuid=command.correlation_uuid,
                                              status=ComputeCommandStatus.FAILED,
                                              message=str(e))

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __info_callback(self, ch, method, props, in_body):
        command: InfoCommand = InfoCommand.model_validate_json(in_body)

        out_body = self.operator.info_enriched().__dict__
        out_body = json.dumps(out_body).encode()

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=BasicProperties(correlation_uuid=str(command.correlation_uuid)),
                         body=out_body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self.channel.basic_consume(queue=self.compute_queue, on_message_callback=self.__compute_callback)
        self.channel.basic_consume(queue=self.info_queue, on_message_callback=self.__info_callback)
        self.channel.start_consuming()
