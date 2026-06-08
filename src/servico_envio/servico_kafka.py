import logging
from typing import Final

from confluent_kafka import Producer
from confluent_kafka.serialization import (
    StringSerializer,
    SerializationContext,
    MessageField
)

from src.config.config import Config
from src.estrategia_serializacao.estrategia_serializacao import EstrategiaSerializacao
from src.modelo.linha import Linha

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger("kafka.avro.producer")


class ProdutorKafka:
    def __init__(self, estrategia_serializacao: EstrategiaSerializacao):
        self.__estrategia_serializacao = estrategia_serializacao

        self.__producer = Producer({
            "bootstrap.servers": f"{Config.URL_KAFKA}:{Config.PORTA_KAFKA}"
        })

        self.__string_serializer = StringSerializer("utf_8")
        self.__TOPICO: Final[str] = "posicoes_sptrans"

    @staticmethod
    def __obter_retorno(err, msg):
        if err is not None:
            logger.error(
                "Erro ao enviar mensagem. tópico=%s erro=%s",
                msg.topic() if msg else "desconhecido",
                err
            )
            return

        logger.info(
            "Mensagem enviada. tópico=%s partição=%s offset=%s",
            msg.topic(),
            msg.partition(),
            msg.offset()
        )

    def enviar_dados(self, dados_envio: Linha):

        # KEY (ok como string simples)
        key = self.__string_serializer(str(dados_envio["cl"]))

        # 🔴 AVRO CORRETO (ESSENCIAL)
        serializer = self.__estrategia_serializacao.serializacao

        value = serializer(
            dados_envio,
            SerializationContext(self.__TOPICO, MessageField.VALUE)
        )

        self.__producer.produce(
            topic=self.__TOPICO,
            key=key,
            value=value,
            on_delivery=self.__obter_retorno
        )

        # não usar flush por mensagem
        self.__producer.poll(0)

    def fechar(self):
        self.__producer.flush()