import json
import random
import uuid
import threading
import time
import pika
from core.settings import settings
from client.src.simple_order import SimpleOrder

class OrderService:
    def __init__(self):
        self.service_id = str(uuid.uuid4())[:8]
        self.orders = {}

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
        print(f"[Pedidos {self.service_id}] Serviço iniciado.")
    
    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_status_exchange',
                                            exchange_type='direct',
                                            durable=True)
        self.channel_consumer.exchange_declare(exchange='entrega_exchange',
                                            exchange_type='topic',
                                            durable=True)
        
        self.pedido_queue = self.channel_consumer.queue_declare(
            queue='pedido_status_queue', durable=True
        ).method.queue
        self.channel_consumer.queue_bind(exchange='pedido_status_exchange',
                                        queue=self.pedido_queue,
                                        routing_key='pedido.status')
        
        self.entrega_queue = self.channel_consumer.queue_declare(
            queue='entrega_queue', durable=True
        ).method.queue 
        self.channel_consumer.queue_bind(exchange='entrega_exchange',
                                        queue=self.entrega_queue,
                                        routing_key='entrega.*')
    
        self.channel_consumer.basic_consume(queue=self.pedido_queue,
                                            on_message_callback=self.order_status_callback,
                                            auto_ack=False)
        self.channel_consumer.basic_consume(queue=self.entrega_queue,
                                            on_message_callback=self.delivery_callback,
                                            auto_ack=False) 

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_publisher.exchange_declare(exchange='pedido_confirmado_exchange',
                                                exchange_type='topic',
                                                durable=True)

    def send_order_confirmation(self, order: SimpleOrder):
        self.channel_publisher.basic_publish(
            exchange='pedido_confirmado_exchange',
            routing_key='pedido.confirmado.todos',
            body=order.model_dump_json(),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))

    def order_status_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)
        
        time.sleep(random.randint(3, 15))
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        if self.orders.get(order_object.order_id) is None:
            self.orders[order_object.order_id] = order_object

        self.update_order_status(order_object.order_id, "RECEBIDO")
        self.print_order_status(order_object)
        
        self.update_order_status(order_object.order_id, "CONFIRMADO")
        self.print_order_status(order_object)
        
        self.send_order_confirmation(order_object)

    def delivery_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)
        
        if order_object.status is not None:
            self.update_order_status(order_object.order_id, order_object.status)

        time.sleep(random.randint(3, 15))
        
        self.print_order_status(order_object)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def update_order_status(self, order_id: str, new_status: str):
        order = self.orders[order_id]
        if order:
            order.status = new_status
            
    def print_order_status(self, order_object: SimpleOrder):
        print(f"[Pedidos {self.service_id}] Pedido {order_object.order_id} {order_object.status}.")

    def listen(self):
        self.channel_consumer.start_consuming()
        print(f"[Pedidos {self.service_id}] Aguardando atualizações...")

    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Pedidos {self.service_id}] Pressione 'q' para sair.\n")
                
                if user_input.lower() == 'q':
                    print(f"[Pedidos {self.service_id}] Encerrando.")
                    break
                
        except KeyboardInterrupt:
            print(f"\n[Pedidos {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection_consumer.close()
            print(f"[Pedidos {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    svc = OrderService()
    svc.run()