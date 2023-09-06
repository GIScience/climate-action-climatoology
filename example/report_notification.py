import pika
from pika import PlainCredentials

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost', port=5672, credentials=PlainCredentials('quest', 'quest')))
channel = connection.channel()
channel.exchange_declare(exchange='notify', exchange_type='fanout')

result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue

channel.queue_bind(exchange='notify', queue=queue_name)


def callback(ch, method, properties, body):
    print(f'Message: {body}')


channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)
channel.start_consuming()
