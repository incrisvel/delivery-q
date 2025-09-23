import pika
import sys
import os


def main():
    credentials = pika.PlainCredentials(
        'lokcpyam', 'HEnWwPtx-hu0UWaaIZl__z3E8kFXqAvI')

    parameters = pika.ConnectionParameters('jackal.rmq.cloudamqp.com',
                                           5672,
                                           'lokcpyam',
                                           credentials)

    connection = pika.BlockingConnection(
        parameters
    )


    channel = connection.channel()

    channel.exchange_declare(
        exchange='pedido_status_exchange', exchange_type='direct')

    channel.queue_declare(queue='pedido_status_queue')

    def callback(ch, method, properties, body):
        print(f" [Pedido] Recebido: {body}")

    channel.basic_consume(queue='pedido_status_queue',
                          on_message_callback=callback, auto_ack=True)

    print(' [Pedido] Esperando por pedidos. Para interromper pressione CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
