import pika
import json
import sys


connection=pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel=connection.channel()


if sys.argv[1]=='notify':
    queue1=channel.queue_declare('order_notify')
    queue_name1=queue1.method.queue


    channel.queue_bind(
        exchange='order',
        queue=queue_name1,
        routing_key='order.notify'
    )


    def callback1(ch, method, properties, body):
        payload=json.loads(body)
        print(' [x] notifying {}'.format(payload['user_email']))
        print(' [x] done')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(on_message_callback=callback1, queue=queue_name1)

elif sys.argv[1]=='report':
    queue2=channel.queue_declare('order_report')
    queue_name2=queue2.method.queue

    channel.queue_bind(
        exchange='order',
        queue=queue_name2,
        routing_key='order.report'
    )


    def callback2(ch, method, properties, body):
        payload=json.loads(body)
        print(' [x] generating report')
        print(f"""
        ID: {payload.get('id')}
        User Email: {payload.get('user_email')}
        Product: {payload.get('product')}
        Quantity: {payload.get('quantity')}
            """)
        print(' [x] done')
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(on_message_callback=callback2, queue=queue_name2)

print(' [*] waiting for notify messages. To exit press CTR+C')

channel.start_consuming()