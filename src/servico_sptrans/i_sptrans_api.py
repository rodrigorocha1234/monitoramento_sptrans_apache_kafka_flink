from typing import Protocol, Union

from src.modelo.linha import Linha


class ISptransApi(Protocol):

    def buscar_linhas(self) -> Union[list[Linha] | None]: ...
