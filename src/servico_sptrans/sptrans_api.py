from datetime import datetime, timezone, timedelta
from itertools import chain
from typing import List, Final, Any, Union

import requests

from src.config.config import Config
from src.excecao.error_login_sptrans import ErroLoginSPTrans
from src.modelo.linha import Linha


class ApiSptrans:
    def __init__(self):
        self.__CHAVE = Config.CHAVE_API_SPTRANS
        self.__URL = Config.URL_API_SPTRANS
        self.__cookie = None  # cache do cookie

    def __realizar_login(self) -> str:

        url_completa: Final[str] = f"{self.__URL}Login/Autenticar?token={self.__CHAVE}"
        resp = requests.post(url_completa, timeout=10)

        if resp.status_code != 200:
            raise ErroLoginSPTrans(f"Erro ao logar na API da SPTrans: {resp.status_code}")

        cookie = resp.cookies.get("apiCredentials")

        if not cookie:
            raise ErroLoginSPTrans("API SPTrans retornou login sem cookie válido.")

        self.__cookie = cookie
        return cookie

    def __get_cookie(self) -> str:

        if self.__cookie:
            return self.__cookie
        return self.__realizar_login()

    @staticmethod
    def __desnormalizar_json(ln: dict) -> List[Linha]:
        return list(
            map(lambda vs_item: Linha(c=ln["c"], cl=ln["cl"], sl=ln["sl"], lt0=ln["lt0"], lt1=ln["lt1"], qv=ln["qv"],
                                      p=vs_item["p"], a=vs_item["a"], ta=int(
                    datetime.fromisoformat(vs_item["ta"].replace("Z", "+00:00")).astimezone(
                        timezone(timedelta(hours=-3))).timestamp() * 1000),
                                      ta_tempo=datetime.fromisoformat(vs_item["ta"].replace("Z", "+00:00")).astimezone(
                                          timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M:%S"),
                                      py=vs_item["py"], px=vs_item["px"], ), ln["vs"], ))

    def buscar_linhas(self) -> Union[list[Any] | None]:

        cookie = self.__get_cookie()
        url_completa: Final[str] = f"{self.__URL}Posicao"
        headers = {'Cookie': f'apiCredentials={cookie}'}

        resp = requests.get(url=url_completa, headers=headers, timeout=10)

        if resp.status_code in [401, 403]:
            print('refazendo login')
            cookie = self.__realizar_login()
            headers['Cookie'] = f'apiCredentials={cookie}'
            resp = requests.get(url=url_completa, headers=headers)

        resp.raise_for_status()

        resposta = resp.json()
        if resposta:
            json_desnormalizado = list(chain.from_iterable(map(self.__desnormalizar_json, resposta['l'])))
            return json_desnormalizado
        return None


if __name__ == "__main__":
    api_sptrans = ApiSptrans()
    linhas = api_sptrans.buscar_linhas()
    for linha in linhas:
        print(linha)
        print()
