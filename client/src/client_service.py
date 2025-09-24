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

        self.connection = pika.BlockingConnection(parameters)

        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange='pedido_status_exchange',
            exchange_type='direct',
            durable=True
          )
        
        print(f"[Cliente {self.service_id}] Serviço iniciado. Aguardando pedidos...")

    def send_order(self):
        order = SimpleOrder()
        
        self.channel.basic_publish(exchange='pedido_status_exchange',
                                   routing_key='pedido.status',
                                   body=f'Pedido: {order.__str__()}',
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.DeliveryMode.Persistent
                                   ))

        print(f"[Cliente {self.service_id}] Pedido de {order.quantity} {order.product} enviado, id: {order.order_id}.")


    def listen(self):
        print(f"[Cliente {self.service_id}] Aguardando atualizações...")


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
            self.connection.close()
            print(f"[Cliente {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    client = ClientService()
    client.run()
    