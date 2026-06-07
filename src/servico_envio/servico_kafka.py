from typing import Final

from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

from config.config import Config
from estrategia_serializacao.estrategia_serializacao import EstrategiaSerializacao
import logging

logging.basicConfig(level=logging.INFO, format=("%(asctime)s - "
                                                "%(name)s - "
                                                "%(levelname)s - "
                                                "%(message)s"))

logger = logging.getLogger("kafka.avro.producer")

class ProdutorKafka:
    def __init__(self, estrategia_serializacao: EstrategiaSerializacao):
        self.__estrategia_serializacao = estrategia_serializacao
        self.__url_kafka = Config.URL_KAFKA
        self.__porta_kafka = Config.PORTA_KAFKA
        self.__string_sertializer = StringSerializer("utf_8")
        self.__producer = Producer({"bootstrap.servers": f"{Config.URL_KAFKA}:{Config.PORTA_KAFKA}"})
        self.__TOPICO: Final[str] = 'posicoes_sptrans'

    @staticmethod
    def __obter_retorno(err, msg):
        if err is not None:
            logger.error("Erro ao enviar mensagem para o tópico %s: %s", msg.topic() if msg else "desconhecido", err)
            return

        logger.info("Mensagem enviada. tópico=%s partição=%s offset=%s", msg.topic(), msg.partition(), msg.offset())

    def enviar_dados(self, dados: dict):
        self.__producer.produce(
            topic=self.__TOPICO,
            key=self.__string_sertializer("100"),
            value=self.__estrategia_serializacao.serializacao(
                user, SerializationContext(self.__TOPICO, MessageField.VALUE)),
            on_delivery=self.__obter_retorno

        )
