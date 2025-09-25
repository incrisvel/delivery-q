from random import random
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
        print(f"[Cliente {self.service_id}] Serviço iniciado. Aguardando pedidos...")
        
    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        
        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_exchange',
                                            exchange_type='topic',
                                            durable=True)
        
        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_dlx',
                                            exchange_type='fanout',
                                            durable=True)
        
        self.channel_consumer.exchange_declare(exchange='entrega_exchange',
                                            exchange_type='topic',
                                            durable=True)
        
        self.channel_consumer.exchange_declare(exchange='entrega_dlx',
                                            exchange_type='fanout',
                                            durable=True)
        
        args_pedido_confirmado = {
            'x-message-ttl': 30000,
            'x-dead-letter-exchange': 'pedido_confirmado_dlx'
        }

        args_entrega = {
            'x-message-ttl': 30000,
            'x-dead-letter-exchange': 'entrega_dlx'
        }
        
        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='pedido_confirmado_queue', durable=True, arguments=args_pedido_confirmado
        ).method.queue

        self.entrega_queue = self.channel_consumer.queue_declare(
            queue='entrega_queue', durable=True, arguments=args_entrega
        ).method.queue
        
        self.channel_consumer.queue_bind(exchange='pedido_confirmado_exchange',
                                        queue=self.pedido_confirmado_queue,
                                        routing_key='pedido.confirmado.cliente')
        
        self.channel_consumer.queue_bind(exchange='entrega_exchange',
                                        queue=self.entrega_queue,
                                        routing_key='entrega.notificar')
        
        self.pedido_confirmado_dead_queue = self.channel_consumer.queue_declare(
            queue='pedido_confirmado_dead_queue', durable=True
        ).method.queue

        self.entrega_dead_queue = self.channel_consumer.queue_declare(
            queue='entrega_dead_queue', durable=True
        ).method.queue
        
        self.channel_consumer.queue_bind(exchange='pedido_confirmado_dlx',
                                        queue=self.pedido_confirmado_dead_queue)
        
        self.channel_consumer.queue_bind(exchange='entrega_dlx',
                                        queue=self.entrega_dead_queue)
        
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_queue,
                                            on_message_callback=self.order_confirmed_callback,
                                            auto_ack=False)
        
        self.channel_consumer.basic_consume(queue=self.entrega_queue,
                                            on_message_callback=self.delivery_callback,
                                            auto_ack=False) 
        
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_dead_queue,
                                        on_message_callback=self.dead_letter_order_confirmed_callback,
                                        auto_ack=True)
        
        self.channel_consumer.basic_consume(queue=self.entrega_dead_queue,
                                        on_message_callback=self.dead_letter_delivery_callback,
                                        auto_ack=True)

    def __producer_service_setup(self, parameters):
        self.connection_publisher = pika.BlockingConnection(parameters)
        self.channel_publisher = self.connection_publisher.channel()
        
        self.channel_publisher.exchange_declare(
            exchange='pedido_status_exchange',
            exchange_type='direct',
            durable=True
          )

    def send_order(self):
        order = SimpleOrder.create_random()
        
        self.channel_publisher.basic_publish(exchange='pedido_status_exchange',
                                   routing_key='pedido.status',
                                   body=order.model_dump_json(),
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.DeliveryMode.Persistent
                                   ))

        print(f"[Cliente {self.service_id}] Pedido de {order.product} enviado, id: {order.order_id}.")


    def order_confirmed_callback(self, ch, method, properties, body):
        print(f"[Cliente {self.service_id}] Mensagem recebida: {body.decode()}")
        time.sleep(random.randint(3, 15))
        ch.basic_ack(delivery_tag=method.delivery_tag)

        self.send_order_confirmation(body)

    def delivery_callback(self, ch, method, properties, body):
        print(f"[Cliente {self.service_id}] Mensagem recebida: {body.decode()}")
        time.sleep(random.randint(3, 15))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def dead_letter_order_confirmed_callback(self, ch, method, properties, body):
        print(f"[Cliente {self.service_id}] DEAD LETTER: {body.decode()}")

    def dead_letter_delivery_callback(self, ch, method, properties, body):
        print(f"[Cliente {self.service_id}] DEAD LETTER: {body.decode()}")

    def listen(self):
        self.channel_consumer.start_consuming()

    def run(self):
        
        threading.Thread(target=self.listen, daemon=True).start()
        try:
            while True:
                user_input = input(
                    f"[Cliente {self.service_id}] Pressione Enter para fazer um pedido ou 'q' para sair: ")
                
                if user_input.lower() == 'q':
                    print(f"[Cliente {self.service_id}] Encerrando.")
                    break
                
                self.send_order()
                
        except KeyboardInterrupt:
            print(f"\n[Cliente {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection_consumer.close()
            self.connection_publisher.close()
            print(f"[Cliente {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    client = ClientService()
    client.run()
    