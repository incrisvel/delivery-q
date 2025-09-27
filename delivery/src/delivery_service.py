import json
import random
import time
import pika
import uuid
import threading
from client.src.simple_order import SimpleOrder
from core.settings import settings

class DeliveryService:
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
        print(f"[Entregas {self.service_id}] Serviço iniciado.")

    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_exchange',
                                            exchange_type='topic',
                                            durable=True)
        
        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='confirmado_entregador_queue', durable=True
        ).method.queue
        self.channel_consumer.queue_bind(exchange='pedido_confirmado_exchange',
                                        queue=self.pedido_confirmado_queue,
                                        routing_key='pedido.confirmado.*')     
        
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_queue,
                                            on_message_callback=self.order_confirmed_callback,
                                            auto_ack=False)

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_publisher.exchange_declare(exchange='entrega_exchange',
                                                exchange_type='topic',
                                                durable=True)

    def send_delivery(self, order: SimpleOrder):
        if order.status != "CONFIRMADO":
            print(f"[Entregas {self.service_id}] Pedido {order.order_id} não confirmado.")
            return
                  
        order_id = order.order_id    
        self.update_order_status(order_id, "EM ROTA")
        self.print_order_status(order_id)
        
        self.channel_publisher.basic_publish(
            exchange='entrega_exchange',
            routing_key='entrega.todos',
            body=self.orders[order.order_id].model_dump_json(),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))
            
        time.sleep(random.randint(15, 25))
    
        order_id = order.order_id 
        self.update_order_status(order_id, "ENTREGUE")
        self.print_order_status(order_id)
        
        self.channel_publisher.basic_publish(
            exchange='entrega_exchange',
            routing_key='entrega.todos',
            body=self.orders[order.order_id].model_dump_json(),
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent
            ))              

    def order_confirmed_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)
        
        if self.orders.get(order_object.order_id) is None:
            self.orders[order_object.order_id] = order_object
            
        if self.orders[order_object.order_id].status in ["RECEBIDO", "FINALIZADO"]:
            print(f"[Entregas {self.service_id}] Pedido {order_object.order_id} já foi finalizado.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return
        
        time.sleep(random.randint(3, 15))
        
        order_id = order_object.order_id
        self.update_order_status(order_id, order_object.status)    
        self.print_order_status(order_id)

        time.sleep(random.randint(3, 15))
        
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_delivery(order_object)
        
    def update_order_status(self, order_id: str, new_status: str):
        order = self.orders[order_id]
        if order:
            order.status = new_status

    def print_order_status(self, order_id: str):
        order_object = self.orders[order_id]
        if order_object is not None:
            print(f"[Entregas {self.service_id}] Pedido {order_object.order_id} {order_object.status}.")

    def listen(self):
        self.channel_consumer.start_consuming()
        print(f"[Entregas {self.service_id}] Aguardando atualizações...")

    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Entregas {self.service_id}] Pressione 'q' para sair.\n")
                
                if user_input.lower() == 'q':
                    print(f"[Entregas {self.service_id}] Encerrando.")
                    break
                
        except KeyboardInterrupt:
            print(f"\n[Entregas {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection_consumer.close()
            print(f"[Entregas {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    svc = DeliveryService()
    svc.run()