from time import sleep
from typing import List

from src.estrategia_serializacao.estrategia_avro import EstrategiaAvro
from src.modelo.linha import Linha
from src.servico_envio.i_servico_envio import IServicoEnvio
from src.servico_envio.servico_kafka import ProdutorKafka
from src.servico_sptrans.i_sptrans_api import ISptransApi
from src.servico_sptrans.sptrans_api import ApiSptrans


class BasePipeline:
    def __init__(self, servico_sptrans_api: ISptransApi, servico_streaming: IServicoEnvio[Linha]):
        self.__servico_sptrans_api = servico_sptrans_api
        self.__servico_streaming = servico_streaming

    def __recuperar_dados_onibus(self) -> List[Linha]:
        lista_linha = self.__servico_sptrans_api.buscar_linhas()
        return lista_linha if lista_linha else []




    def rodar_pipeline(self):
        linhas = self.__recuperar_dados_onibus()
        while True:
            if linhas:
                for linha in linhas:
                    self.__servico_streaming.enviar_dados(linha)
            else:
                print('Sem resultado')
            sleep(60)


if __name__ == "__main__":
    api_sptrans = ApiSptrans()

    estrategia = EstrategiaAvro()
    produtor = ProdutorKafka(estrategia)

    pipeline = BasePipeline(servico_sptrans_api=api_sptrans, servico_streaming=produtor, )
    pipeline.rodar_pipeline()
