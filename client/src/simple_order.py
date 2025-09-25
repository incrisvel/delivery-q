import uuid
from random import randint, choice
from pydantic import BaseModel
from typing import Optional

class SimpleOrder(BaseModel):
    order_id: str
    product: str
    quantity: int
    unit_price: float
    status: Optional[str] = None
    
    @classmethod
    def create_random(cls):
        return cls(
            order_id = str(uuid.uuid4())[:6],
            product = choice(["macarrão", "requeijão", "motosserra", "tinta de parede", "cadeira", "cadeira de rodas gamer"]),
            quantity = randint(1, 1000),
            unit_price = round(randint(100, 10000) / 100, 2),
            status = None
        )
    