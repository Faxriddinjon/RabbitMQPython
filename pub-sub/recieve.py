import pika
import sys



connection=pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel=connection.channel()

queue=channel.queue_declare(queue='', exclusive=True)
queue_name=queue.method.queue

channel.queue_bind(exchange='logs', queue=queue_name)

print(' [*] waiting for logs. to exit press CTRL+C')

def callback(ch, method, propersties, body):
    print(f" [x] {body}")


channel.basic_consume(
    queue=queue_name,
    on_message_callback=callback,
    auto_ack=True
)

channel.start_consuming()