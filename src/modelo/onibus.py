from datetime import datetime
from typing import TypedDict


class Onibus(TypedDict):
    p: int
    a: bool
    ta: datetime
    py: float
    px: float
