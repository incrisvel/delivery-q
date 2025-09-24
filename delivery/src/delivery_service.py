import random
import time
import pika
import uuid
import threading
from core.settings import settings
from order.src.order_service import OrderService

class DeliveryService:
    def __init__(self):
        self.service_id = str(uuid.uuid4())[:8]

        credentials = pika.PlainCredentials(
            settings.rabbitmq_user, 
            settings.rabbitmq_pass
        )

        parameters = pika.ConnectionParameters(
            settings.rabbitmq_host,
            settings.rabbitmq_port,
            settings.rabbitmq_vhost,
            credentials
        )

        self.__consumer_service_setup(parameters)
        self.__producer_service_setup(parameters)

        self._consume_thread = None
        print(f"[Entrega {self.service_id}] Serviço iniciado. Aguardando mensagens...")

    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_exchange',
                                            exchange_type='topic',
                                            durable=True)
        
        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='pedido_confirmado_queue', durable=True
        ).method.queue
        self.channel_consumer.queue_bind(exchange='pedido_confirmado_exchange',
                                        queue=self.pedido_confirmado_queue,
                                        routing_key='pedido.confirmado.entregador')     
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_queue,
                                            on_message_callback=self.order_confirmed_callback,
                                            auto_ack=False)

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_publisher.exchange_declare(exchange='entrega_exchange',
                                                exchange_type='topic',
                                                durable=True)

    def send_delivery(self, received_body):
        self.channel_publisher.basic_publish(
            exchange='entrega_exchange',
            routing_key='entrega.1',
            body=f'Entrega: {received_body.decode()}',
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))

    def order_confirmed_callback(self, ch, method, properties, body):
        print(f"[Entrega {self.service_id}] Pedido mensagem recebida: {body.decode()}")
        time.sleep(random.randint(3, 15))
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_delivery(body)

    def listen(self):
        self.channel_consumer.start_consuming()

    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Entrega {self.service_id}] Pressione 'q' para sair: ")
                
                if user_input.lower() == 'q':
                    print(f"[Entrega {self.service_id}] Encerrando.")
                    break
                
        except KeyboardInterrupt:
            print(f"\n[Entrega {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection_consumer.close()
            print(f"[Entrega {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    svc = DeliveryService()
    svc.run()