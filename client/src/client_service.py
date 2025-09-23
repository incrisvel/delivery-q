import os
from dotenv import load_dotenv
import pika
import uuid
import threading
from simple_order import SimpleOrder

class ClientService:
    def __init__(self):
        self.client_id = str(uuid.uuid4())[:8]

        load_dotenv()

        credentials = pika.PlainCredentials(
            os.getenv('RABBITMQ_USER'), os.getenv('RABBITMQ_PASS') )

        parameters = pika.ConnectionParameters(os.getenv('RABBITMQ_HOST'),
                                               os.getenv('RABBITMQ_PORT'),
                                               os.getenv('RABBITMQ_VHOST'),
                                               credentials)

        self.connection = pika.BlockingConnection(parameters)

        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange='pedido_status_exchange',
            exchange_type='direct'
          )
        
        print(f"[Cliente {self.client_id}] Serviço iniciado. Aguardando pedidos...")

    def send_order(self):
        order = SimpleOrder();
        
        self.channel.basic_publish(exchange='pedido_status_exchange',
                                   routing_key='pedido.status',
                                   body=str(order),
                                   properties=pika.BasicProperties(
                                       delivery_mode=pika.DeliveryMode.Persistent
                                   ))

        print(f"[Cliente {self.client_id}] Pedido de {order.product} enviado, id: {order.order_id}.")


    def listen(self):
        print(f"[Cliente {self.client_id}] Aguardando atualizações...")


    def run(self):
        
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Cliente {self.client_id}] Pressione Enter para fazer um pedido ou 'q' para sair: ")
                
                if user_input.lower() == 'q':
                    print(f"[Cliente {self.client_id}] Encerrando.")
                    break
                
                self.send_order()
                
        except KeyboardInterrupt:
            print(f"\n[Cliente {self.client_id}] Keyboard interruption.")
        
        finally:
            self.connection.close()
            print(f"[Cliente {self.client_id}] Conexão fechada.")

if __name__ == '__main__':
    client = ClientService()
    client.run()
