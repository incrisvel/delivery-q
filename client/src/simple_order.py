from random import randint, choice
import uuid

class SimpleOrder:
    def __init__(self):
        self.order_id = str(uuid.uuid4())[:6]
        self.product = choice(["macarrão", "requeijão", "motosserra", "tinta de parede", "cadeira", "cadeira de rodas gamer"])
        self.quantity = randint(1, 1000)
        
        if self.quantity> 1:
            self.product+= "s"