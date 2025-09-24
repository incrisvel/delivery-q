import os
import random
import uuid
import json
import threading
import time
from dotenv import load_dotenv
import pika

class OrderService:
    def __init__(self):
        self.service_id = str(uuid.uuid4())[:8]
        load_dotenv()

        credentials = pika.PlainCredentials(
            os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASS')
        )

        parameters = pika.ConnectionParameters(os.getenv('RABBITMQ_HOST'),
                                               os.getenv('RABBITMQ_PORT'),
                                               os.getenv('RABBITMQ_VHOST'),
                                               credentials)
        
        self.__consumer_service_setup(parameters)
        self.__producer_service_setup(parameters)

        self._consume_thread = None
        print(f"[OrderService {self.service_id}] Iniciado. Aguardando mensagens...")
    
    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_status_exchange',
                                               exchange_type='direct',
                                               durable=True)
        self.channel_consumer.exchange_declare(exchange='entrega_exchange',
                                               exchange_type='topic',
                                               durable=True)
        
        self.pedido_queue = self.channel_consumer.queue_declare(queue='pedido_status_queue', exclusive=True).method.queue
        self.channel_consumer.queue_bind(exchange='pedido_status_exchange',
                                         queue=self.pedido_queue,
                                         routing_key='pedido.status')
        self.entrega_queue = self.channel_consumer.queue_declare(queue='entrega_queue', exclusive=True).method.queue
        self.channel_consumer.queue_bind(exchange='entrega_exchange',
                                         queue=self.entrega_queue,
                                         routing_key='entrega.status')
        
        self.channel_consumer.basic_consume(queue=self.pedido_queue,
                                            on_message_callback=self.on_pedido_status,
                                            auto_ack=False)
        self.channel_consumer.basic_consume(queue=self.entrega_queue,
                                            on_message_callback=self.on_entrega_status,
                                            auto_ack=False)

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_publisher.exchange_declare(exchange='pedido_confirmado_exchange',
                                                exchange_type='topic',
                                                durable=True)

    def send_order_confirmation(self, received_body):
        self.channel_publisher.basic_publish(
            exchange='pedido_confirmado_exchange',
            routing_key='pedido.confirmado.*',
            body=str('Confirmado: ', received_body),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))

    def order_status_callback(self, ch, method, body):
        print(f"[OrderService {self.service_id}] Mensagem recebida: {body.decode()}")
        time.sleep(random.randint(3, 15))
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_order_confirmation(body)

    def delivery_callback(self, ch, method, body):
        print(f"[OrderService {self.service_id}] Mensagem recebida: {body.decode()}")
        time.sleep(random.randint(3, 15))
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def listen(self):
        self.channel_consumer.basic_consume(queue='pedido_status_queue', on_message_callback=order_status_callback)
        self.channel_consumer.basic_consume(queue='entrega_queue', on_message_callback=delivery_callback)
        self.channel_consumer.start_consuming()

    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()

if __name__ == '__main__':
    svc = OrderService()
    svc.run()