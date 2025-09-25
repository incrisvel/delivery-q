import json
import random
import time
import pika
import uuid
import threading
from .simple_order import SimpleOrder
from core.settings import settings

class ClientService:
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
        print(f"[Clientes {self.service_id}] Serviço iniciado. Aguardando pedidos...")
        

    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_publisher.exchange_declare(exchange='entrega_exchange',
                                                exchange_type='topic',
                                                durable=True)
        self.channel_publisher.exchange_declare(exchange='pedido_confirmado_exchange',
                                                exchange_type='topic',
                                                durable=True)
        
        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='notificar_queue', durable=True
        ).method.queue
        self.channel_consumer.queue_bind(exchange='entrega_exchange',
                                        queue=self.pedido_confirmado_queue,
                                        routing_key='entrega.notificar')     
        
        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='confirmado_cliente_queue', durable=True
        ).method.queue
        self.channel_consumer.queue_bind(exchange='pedido_confirmado_exchange',
                                        queue=self.pedido_confirmado_queue,
                                        routing_key='pedido.confirmado.cliente') 
        
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_queue,
                                            on_message_callback=self.delivery_notification_callback,
                                            auto_ack=False)    
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_queue,
                                            on_message_callback=self.order_confirmed_callback,
                                            auto_ack=False)

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_status_exchange',
                                               exchange_type='direct',
                                               durable=True
        )
 
    def delivery_notification_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)
        
        time.sleep(random.randint(3, 15))
        print(f"[Clientes {self.service_id}] Notificação de entrega recebida, pedido {order_object.order_id}. Status: {order_object.status}")
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def order_confirmed_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)
        
        time.sleep(random.randint(3, 15))
        print(f"[Clientes {self.service_id}] Notificação de confirmação do pedido {order_object.order_id}. Status: {order_object.status}")

        
        ch.basic_ack(delivery_tag=method.delivery_tag)
 
    def send_order(self):
        order = SimpleOrder.create_random()
        
        self.channel_consumer.basic_publish(exchange='pedido_status_exchange',
                                   routing_key='pedido.status',
                                   body=order.model_dump_json(),
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.DeliveryMode.Persistent
                                   ))

        print(f"[Clientes {self.service_id}] Pedido de {order.product} enviado, id: {order.order_id}.")
        

    def listen(self):
        print(f"[Clientes {self.service_id}] Aguardando atualizações...")


    def run(self):
        
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Clientes {self.service_id}] Pressione Enter para fazer um pedido ou 'q' para sair: ")
                
                if user_input.lower() == 'q':
                    print(f"[Clientes {self.service_id}] Encerrando.")
                    break
                
                self.send_order()
                
        except KeyboardInterrupt:
            print(f"\n[Clientes {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection_consumer.close()
            print(f"[Clientes {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    client = ClientService()
    client.run()