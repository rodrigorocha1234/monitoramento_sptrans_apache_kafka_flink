from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class EstrategiaSerializacao(ABC, Generic[T]):

    def __init__(self):
        self._serializacao: T | None = None

    @property
    def serializacao(self) -> T:
        if self._serializacao is None:
            self._serializacao = self.inicializar_serializacao()

        return self._serializacao

    @abstractmethod
    def inicializar_serializacao(self) -> T:
        pass