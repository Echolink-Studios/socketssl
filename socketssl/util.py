from dataclasses import dataclass

from pydantic import BaseModel

HEADER = 64


class Payload(BaseModel):
    source: str
    destination: str
    data: str


@dataclass
class Response:
    source: str
    destination: str
    data: str
