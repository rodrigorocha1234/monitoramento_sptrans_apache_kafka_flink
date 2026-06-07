from typing import Protocol, TypeVar

DADOS_ENVIO = TypeVar("DADOS_ENVIO", contravariant=True)


class IServicoEnvio(Protocol[DADOS_ENVIO]):

    def enviar_dados(self, dados_envio: DADOS_ENVIO) -> None: ...
