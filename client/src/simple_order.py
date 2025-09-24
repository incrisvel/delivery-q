import json
from random import randint, choice
import uuid

class SimpleOrder:
    def __init__(self):
        self.order_id = str(uuid.uuid4())[:6]
        self.product = choice(["macarrão", "requeijão", "motosserra", "tinta de parede", "cadeira", "cadeira de rodas gamer"])
        self.quantity = randint(1, 1000)
        self.unit_price = round(randint(100, 10000) / 100, 2)
        self.status = None

    
            
    def __str__(self):
        return f"{self.quantity} unidade(s) de {self.product} (id: {self.order_id})"
    
    def build_json(self):
        information = {
            "id_pedido": self.order_id,
            "produto": self.product,
            "quantidade": self.quantity,
            "preco_unitario": self.unit_price,
            "status": self.status
        }
        return json.dumps(information)
    
    def build_object(json_str):
        data = json.loads(json_str)
        order = SimpleOrder()
        order.order_id = data["id_pedido"]
        order.product = data["produto"]
        order.quantity = data["quantidade"]
        order.unit_price = data["preco_unitario"]
        order.status = data["status"]
        return order