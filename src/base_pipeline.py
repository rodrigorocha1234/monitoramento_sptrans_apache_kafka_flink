from typing import List

from modelo.linha import Linha
from servico_envio.i_servico_envio import IServicoEnvio
from src.servico_sptrans.i_sptrans_api import ISptransApi
from src.servico_sptrans.sptrans_api import ApiSptrans


class BasePipeline:
    def __init__(self, servico_sptrans_api: ISptransApi, servico_streaming: IServicoEnvio[Linha]):
        self.__servico_sptrans_api = servico_sptrans_api
        self.__servico_streaming = servico_streaming

    def __recuperar_dados_onibus(self) -> List[Linha]:
        lista_linha = self.__servico_sptrans_api.buscar_linhas()
        return lista_linha

    def __enviar_dados(self, lista_linhas: List[Linha]):
        for linha in lista_linhas:
            print(linha)

        # while True:  #     for linha in lista_linhas:  #         self.__servico_streaming.enviar_dados(linha)  #     sleep(60)

    def rodar_pipeline(self):
        linhas = self.__recuperar_dados_onibus()
        if linhas:
            for linha in linhas:
                print(linha)
        else:
            print('Sem resultado')

if __name__ == "__main__":
    api_sptrans = ApiSptrans()

    pipeline = BasePipeline(servico_sptrans_api=api_sptrans, servico_streaming=None, )
    pipeline.rodar_pipeline()
