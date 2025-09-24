import pika
import uuid
import threading
from core.settings import settings

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

        self.connection = pika.BlockingConnection(parameters)

        self.channel = self.connection.channel()

        self.channel.exchange_declare(
            exchange='pedido_status_exchange', exchange_type='direct')

        self.channel.queue_declare(queue='pedido_status_queue')

        def callback(ch, method, properties, body):
            print(f"[Pedido {self.service_id}] Recebido: {body}")

        self.channel.basic_consume(queue='pedido_status_queue',
                              on_message_callback=callback, auto_ack=True)

        print('[Pedido] Esperando por pedidos...')
        self.channel.start_consuming()


    def listen(self):
        print(f"[Pedido {self.service_id}] Aguardando atualizações...")


    def run(self):
        
        threading.Thread(target=self.listen, daemon=True).start()
        
        try:
            while True:
                user_input = input(
                    f"[Pedido {self.service_id}] Pressione 'q' para sair: ")
                
                if user_input.lower() == 'q':
                    print(f"[Pedido {self.service_id}] Encerrando.")
                    break
                
                self.send_order()
                
        except KeyboardInterrupt:
            print(f"\n[Pedido {self.service_id}] Keyboard interruption.")
        
        finally:
            self.connection.close()
            print(f"[Pedido {self.service_id}] Conexão fechada.")

if __name__ == '__main__':
    deliveryService = DeliveryService()
    deliveryService.run()