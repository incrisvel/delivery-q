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
        print(f"[Clientes {self.service_id}] Serviço iniciado.")
        

    def __consumer_service_setup(self, parameters):
        self.connection_consumer = pika.BlockingConnection(parameters)
        self.channel_consumer = self.connection_consumer.channel()
        self.__set_entrega_service()
        self.__set_pedido_confirmado_service()

    def __set_entrega_service(self):
        self.channel_consumer.exchange_declare(exchange='entrega_dlx',
                                            exchange_type='fanout',
                                            durable=True)

        self.entrega_dead_queue = self.channel_consumer.queue_declare(
            queue='entrega_dead_queue', durable=True
        ).method.queue

        self.channel_consumer.queue_bind(exchange='entrega_dlx',
                                        queue=self.entrega_dead_queue)

        self.channel_consumer.basic_consume(queue=self.entrega_dead_queue,
                                        on_message_callback=self.dl_delivery_callback,
                                        auto_ack=False)
        arguments = {
            'x-message-ttl': 30000,
            'x-dead-letter-exchange': 'entrega_dlx'
        }
    
        self.channel_consumer.exchange_declare(exchange='entrega_exchange',
                                        exchange_type='topic',
                                        durable=True)
        
        self.notificar_queue = self.channel_consumer.queue_declare(
            queue='notificar_queue', durable=True, arguments=arguments
        ).method.queue
        
        self.channel_consumer.queue_bind(exchange='entrega_exchange',
                                queue=self.notificar_queue,
                                routing_key='entrega.*')
        
        self.channel_consumer.basic_consume(queue=self.notificar_queue,
                                    on_message_callback=self.delivery_notification_callback,
                                    auto_ack=False)
        
    def __set_pedido_confirmado_service(self):
        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_dlx',
                                            exchange_type='fanout',
                                            durable=True)

        self.pedido_confirmado_dead_queue = self.channel_consumer.queue_declare(
            queue='pedido_confirmado_dead_queue', durable=True
        ).method.queue

        self.channel_consumer.queue_bind(exchange='pedido_confirmado_dlx',
                                        queue=self.pedido_confirmado_dead_queue)
        
        self.channel_consumer.basic_consume(queue=self.pedido_confirmado_dead_queue,
                                        on_message_callback=self.dl_order_confirmed_callback,
                                        auto_ack=False)
        
        arguments = {
            'x-message-ttl': 30000,
            'x-dead-letter-exchange': 'pedido_confirmado_dlx'
        }

        self.channel_consumer.exchange_declare(exchange='pedido_confirmado_exchange',
                                                exchange_type='topic',
                                                durable=True)

        self.pedido_confirmado_queue = self.channel_consumer.queue_declare(
            queue='confirmado_cliente_queue', durable=True, arguments=arguments
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
        
        self.channel_publisher.exchange_declare(exchange='pedido_status_exchange',
                                               exchange_type='direct',
                                               durable=True
        )
 
    def delivery_notification_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)

        if self.orders.get(order_object.order_id) is None:
            self.orders[order_object.order_id] = order_object
            
        if order_object.status == "RECEBIDO":
            print(f"[Clientes {self.service_id}] Pedido {order_object.order_id} já foi recebido.")
            return
        
        time.sleep(random.randint(3, 15))

        if order_object.status == "ENTREGUE":
            order_id = order_object.order_id
            self.update_order_status(order_id, "RECEBIDO")
            self.print_order_status(order_id)
           
            self.channel_publisher.basic_publish(exchange='pedido_status_exchange',
                            routing_key='pedido.status',
                            body=self.orders[order_object.order_id].model_dump_json(),
                            properties=pika.BasicProperties(
                                delivery_mode=pika.DeliveryMode.Persistent
            ))
        else:
            order_id = order_object.order_id
            self.update_order_status(order_id, order_object.status)
            self.print_order_status(order_id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
        
    def order_confirmed_callback(self, ch, method, properties, body):
        order_json = json.loads(body)
        order_object = SimpleOrder(**order_json)

        if self.orders.get(order_object.order_id) is None:
            self.orders[order_object.order_id] = order_object
        
        time.sleep(random.randint(3, 15))
        
        order_id = order_object.order_id
        self.update_order_status(order_id, order_object.status)
        self.print_order_status(order_id)
        
        ch.basic_ack(delivery_tag=method.delivery_tag)
 
    def send_order(self):
        order = SimpleOrder.create_random()

        if self.orders.get(order.order_id) is not None:
            print(f"[Entregas {self.service_id}] Pedido {order.order_id} já existe.")
            return

        self.orders[order.order_id] = order
        
        self.channel_publisher.basic_publish(exchange='pedido_status_exchange',
                                   routing_key='pedido.status',
                                   body=order.model_dump_json(),
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.DeliveryMode.Persistent
                                   ))
        
        order_id = order.order_id
        self.update_order_status(order_id, "ENVIADO")
        self.print_order_status(order_id)


    def _extract_original_routing_key(self, properties, default):
        try:
            headers = properties.headers or {}
            x_death = headers.get('x-death')
            if x_death and isinstance(x_death, list) and len(x_death) > 0:
                first = x_death[0]
                if 'routing-keys' in first and isinstance(first['routing-keys'], list) and len(first['routing-keys']) > 0:
                    return first['routing-keys'][0]
                if 'routing_key' in first:
                    return first['routing_key']
        except Exception:
            pass
        return default

    def dl_delivery_callback(self, ch, method, properties, body):
        routing_key = self._extract_original_routing_key(properties, default='entrega.retry')
        try:
            self.channel_publisher.basic_publish(
                exchange='entrega_exchange',
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    headers=properties.headers
                )
            )
            print(f"[Clientes {self.service_id}] Mensagem da DLQ entrega republicada para entrega_exchange ({routing_key}).")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[Clientes {self.service_id}] Falha ao republicar da DLQ entrega: {e}. Requeue na DLQ.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def dl_order_confirmed_callback(self, ch, method, properties, body):
        routing_key = self._extract_original_routing_key(properties, default='pedido.confirmado.retry')
        try:
            self.channel_publisher.basic_publish(
                exchange='pedido_confirmado_exchange',
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent,
                    headers=properties.headers
                )
            )
            print(f"[Clientes {self.service_id}] Mensagem da DLQ pedido_confirmado republicada para pedido_confirmado_exchange ({routing_key}).")
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"[Clientes {self.service_id}] Falha ao republicar da DLQ pedido_confirmado: {e}. Requeue na DLQ.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    
    def update_order_status(self, order_id: str, new_status: str):
        order = self.orders[order_id]
        if order:
            order.status = new_status
            
    def print_order_status(self, order_id: str):
        order_object = self.orders[order_id]
        if order_object is not None:
            print(f"[Clientes {self.service_id}] Pedido {order_object.order_id} {order_object.status}.")

    def listen(self):
        self.channel_consumer.start_consuming()
        print(f"[Clientes {self.service_id}] Aguardando atualizações...")

    def run(self):
        
        consumer_thread = threading.Thread(target=self.listen, daemon=True)
        consumer_thread.start()
        
        try:
            print(f"[Clientes {self.service_id}] Pressione Enter para fazer um pedido ou 'q' para sair.")

            while True:
                user_input = input()
                
                if user_input.lower() == 'q':
                    print(f"[Clientes {self.service_id}] Encerrando.")
                    break
                
                self.send_order()
                
        except KeyboardInterrupt:
            print(f"\n[Clientes {self.service_id}] Keyboard interruption.")
        
        finally:
            if self.channel_consumer.is_open:
                self.channel_consumer.connection.add_callback_threadsafe(
                    lambda: self.channel_consumer.stop_consuming()  
                )

            consumer_thread.join()
            
            if self.connection_consumer.is_open:
                self.connection_consumer.close()
            
            print(f"[Clientes {self.service_id}] Conexão fechada.")


if __name__ == '__main__':
    svc = ClientService()
    svc.run()