import pika
import json
import uuid

connection=pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel=connection.channel()

channel.exchange_declare(exchange='order', exchange_type='direct')

order={
    'id': str(uuid.uuid4()),
    'user_email': 'faxriddintopvoldiyev@gmail.com',
    'product': 'thick hat',
    'quantity': 1
}

channel.basic_publish(
    exchange='order',
    routing_key='order.notify',
    body=json.dumps({'user_email': order['user_email']})
)

print(' [x] sent notify message')

channel.basic_publish(
    exchange='order',
    routing_key='order.report',
    body=json.dumps(order)
)

print(' [x] sent report message')

connection.close()