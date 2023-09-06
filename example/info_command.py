import json
import uuid

import pika
from pika import PlainCredentials

from climatoology.base.event import info_command_schema, InfoCommand

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=PlainCredentials('quest', 'quest')))
channel = connection.channel()
result = channel.queue_declare(queue='', exclusive=True)
callback_queue = result.method.queue

command = InfoCommand(uuid.uuid4())


def on_response(ch, method, props, body):
    if str(command.correlation_uuid) == props.correlation_id:
        print(body)


channel.basic_consume(
    queue=callback_queue,
    on_message_callback=on_response,
    auto_ack=True)

channel.basic_publish(exchange='',
                      routing_key='ghg_lulc_info',
                      properties=pika.BasicProperties(reply_to=callback_queue),
                      body=json.dumps(info_command_schema.dump(command)).encode())
connection.process_data_events(time_limit=100)
