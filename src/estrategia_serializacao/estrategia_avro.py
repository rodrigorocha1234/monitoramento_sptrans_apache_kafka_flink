from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from src.config.config import Config
from src.estrategia_serializacao.estrategia_serializacao import EstrategiaSerializacao


class EstrategiaAvro(EstrategiaSerializacao):

    def __init__(self):
        super().__init__()

        self.__schema_registry_client = SchemaRegistryClient(
            {"url": f"http://{Config.URL_SCHEMA_REGISTRY}:{Config.PORTA_SCHEMA_REGISTRY}"})

        self.__schema_path = Config.CAMINHO_SCHEMA

    def to_dict(self, obj, ctx):
        return {
            "c": obj["c"],
            "cl": int(obj["cl"]),
            "sl": int(obj["sl"]),
            "lt0": obj["lt0"],
            "lt1": obj["lt1"],
            "qv": int(obj["qv"]),
            "p": int(obj["p"]),
            "a": bool(obj["a"]),
            "ta": int(obj["ta"]),
            "ta_tempo": obj["ta_tempo"],
            "py": float(obj["py"]),
            "px": float(obj["px"]),
        }
    def inicializar_serializacao(self) -> AvroSerializer:
        with open(self.__schema_path, "r", encoding="utf-8") as file:
            schema_str = file.read()

        return AvroSerializer(self.__schema_registry_client, schema_str, self.to_dict)
