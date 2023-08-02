import json
import uuid

import pika

from climatoology.base.event import ReportCommand, report_command_schema

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672))
channel = connection.channel()
command = ReportCommand(uuid.uuid4(), params={
    'area': [8.674092, 49.417479, 8.778598, 49.430438]
})
channel.basic_publish(exchange='',
                      routing_key='ghg_lulc_report',
                      body=json.dumps(report_command_schema.dump(command)).encode())
