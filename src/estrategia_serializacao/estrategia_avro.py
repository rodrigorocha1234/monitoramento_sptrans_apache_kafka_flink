from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from config.config import Config
from estrategia_serializacao.estrategia_serializacao import (
    EstrategiaSerializacao
)


class AvroStrategy(EstrategiaSerializacao[AvroSerializer]):

    def __init__(self):
        super().__init__()

        self.__schema_registry_client = SchemaRegistryClient(
            {
                "url": (
                    f"http://{Config.URL_SCHEMA_REGISTRY}:"
                    f"{Config.PORTA_SCHEMA_REGISTRY}"
                )
            }
        )

        self.__schema_path = Config.CAMINHO_SCHEMA

    def inicializar_serializacao(self) -> AvroSerializer:
        with open(
            self.__schema_path,
            "r",
            encoding="utf-8"
        ) as file:
            schema_str = file.read()

        return AvroSerializer(
            self.__schema_registry_client,
            schema_str
        )