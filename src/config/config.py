import os
from typing import Final

from dotenv import load_dotenv

load_dotenv()


class Config:
    URL_API_SPTRANS: Final[str] = os.environ['URL_API_SPTRANS']
    CHAVE_API_SPTRANS: Final[str] = os.environ['CHAVE_API_SPTRANS']
    URL_KAFKA: Final[str] = os.environ['URL_KAFKA']
    PORTA_KAFKA: Final[str] = os.environ['PORTA_KAFKA']
    URL_SCHEMA_REGISTRY: Final[str] = os.environ['URL_SCHEMA_REGISTRY']
    PORTA_SCHEMA_REGISTRY: Final[str] = os.environ['PORTA_SCHEMA_REGISTRY']
    CAMINHO_SCHEMA: Final[str] = os.path.join(os.getcwd(),'schema_sptrans', 'schema_sptrans.avsc')
