from dataclasses import dataclass
import json

@dataclass
class ApiResponseRecord:
    identifier: int
    date: str
    quantity: int
    price: float
    

    def to_json(self):
        return json.dumps(self.__dict__)
