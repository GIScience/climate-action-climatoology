import json
from uuid import UUID

from pika import BlockingConnection, BasicProperties

from climatoology.base.event import report_command_schema, ReportCommand, report_command_result_schema, \
    ReportCommandResult, ReportCommandStatus, info_command_schema, InfoCommand
from climatoology.base.operator import Operator
from climatoology.store.object_store import Storage


class PlatformPlugin:
    """Climate Action Platform worker.

    It's responsible for handling user commands and emitting system-wide notifications.
    The main plugin logic and workload is handled by the Operator.
    """

    def __init__(self,
                 operator: Operator,
                 storage: Storage,
                 broker: BlockingConnection,
                 notification_queue='notify'):
        self.operator = operator
        self.storage = storage
        self.broker = broker

        name = operator.info().name.lower()
        self.report_queue = f'{name}_report'
        self.info_queue = f'{name}_info'
        self.notification_queue = notification_queue

        self.channel = self.broker.channel()
        self.channel.basic_qos(prefetch_count=1)

        self.channel.queue_declare(queue=self.report_queue, durable=True)
        self.channel.exchange_declare(self.notification_queue, exchange_type='fanout')
        self.channel.queue_declare(queue=self.info_queue)

    def __publish_notification(self, correlation_id: UUID, status: ReportCommandStatus):
        body = report_command_result_schema.dump(ReportCommandResult(correlation_id, status))
        body = json.dumps(body).encode()
        self.channel.basic_publish(exchange=self.notification_queue, routing_key='', body=body)

    def __report_callback(self, ch, method, _, body):
        command: ReportCommand = report_command_schema.load(json.loads(body))
        self.__publish_notification(command.correlation_uuid, ReportCommandStatus.IN_PROGRESS)

        try:
            artifacts = self.operator.report(command.params)
            self.storage.save_all(artifacts)

            self.__publish_notification(command.correlation_uuid, ReportCommandStatus.COMPLETED)
        except ValueError as ve:
            self.__publish_notification(command.correlation_uuid, ReportCommandStatus.FAILED)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def __info_callback(self, ch, method, props, in_body):
        command: InfoCommand = info_command_schema.load(json.loads(in_body))

        out_body = self.operator.info().__dict__
        out_body = json.dumps(out_body).encode()

        ch.basic_publish(exchange='',
                         routing_key=props.reply_to,
                         properties=BasicProperties(correlation_id=str(command.correlation_uuid)),
                         body=out_body)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def run(self):
        self.channel.basic_consume(queue=self.report_queue, on_message_callback=self.__report_callback)
        self.channel.basic_consume(queue=self.info_queue, on_message_callback=self.__info_callback)
        self.channel.start_consuming()
