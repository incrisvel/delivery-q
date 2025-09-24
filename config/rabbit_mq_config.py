import pika
from core.settings import settings

class RabbitMQConfig:
    
    def __init__(self):
        self.credentials = pika.PlainCredentials(
            settings.rabbitmq_user, 
            settings.rabbitmq_pass
        )

        self.parameters = pika.ConnectionParameters(
            settings.rabbitmq_host,
            settings.rabbitmq_port,
            settings.rabbitmq_vhost,
            self.credentials
        )

        
        self.connection = pika.BlockingConnection(self.parameters)
        self.channel = self.connection.channel()
        self.setup_exchanges()
        self.setup_queues()
        self.setup_bindings()

    def setup_exchanges(self):
        self.channel.exchange_declare(
            exchange='pedido_status_exchange', exchange_type='direct', durable=True)
        self.channel.exchange_declare(
            exchange='pedido_confirmado_exchange', exchange_type='topic', durable=True)
        self.channel.exchange_declare(
            exchange='entrega_exchange', exchange_type='topic', durable=True)
        
    def setup_queues(self):
        self.channel.queue_declare(queue='pedido_status_queue', durable=True)
        self.channel.queue_declare(queue='pedido_confirmado_entregador_queue', durable=True)
        self.channel.queue_declare(queue='pedido_confirmado_cliente_queue', durable=True)
        self.channel.queue_declare(queue='entrega_status_queue', durable=True)
        self.channel.queue_declare(queue='entrega_notificar_queue', durable=True)

    def setup_bindings(self):
        self.channel.queue_bind(
            exchange='pedido_status_exchange', queue='pedido_status_queue', routing_key='pedido.status')
        self.channel.queue_bind(
            exchange='pedido_confirmado_exchange', queue='pedido_confirmado_entregador_queue', routing_key='pedido.confirmado.entregador')
        self.channel.queue_bind(
            exchange='pedido_confirmado_exchange', queue='pedido_confirmado_cliente_queue', routing_key='pedido.confirmado.cliente')
        self.channel.queue_bind(
            exchange='entrega_exchange', queue='entrega_status_queue', routing_key='entrega.status')
        self.channel.queue_bind(
            exchange='entrega_exchange', queue='entrega_notificar_queue', routing_key='entrega.notificar')