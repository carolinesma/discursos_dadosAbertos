import requests
import json
import os
import errno
import csv
import sys
import time
import pandas as pd
from pandas import json_normalize
from typing import Optional

PATH_DISCURSOS = 'Discursos'    #Nome da pasta para salvar os discursos
PATH_SUMARIOS ='Sumários'       #Nome da pasta para salvar as informações dos deputados
MAX_RETRIES = 3                 
WAIT_SECONDS = 10

def makeDir(path) -> None:
    '''
    Rotina para criar diretórios para a organização dos arquivos
    :param path: recebe endereço do diretório
    :return: None
    '''
    try:
        os.makedirs(path, exist_ok = True)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        pass

def json_to_dataFrame(json_data: json, tag: str) -> pd.DataFrame:
    '''
    Função para converter e tratar os dados json do request, ela converte
    json para DataFrame e elimina as informações desnecessárias.
    :param json_data: recebe o json a ser convertido
    :param tag: recebe uma tag indicando qual o tipo de dado a ser tratado
    :return: retorna o dataframe tratado
    '''
    if tag == 'discursos':
        df = json_normalize(json_data['dados'], sep = ';')
        if df.empty:
            return df
        else: return pd.concat([df['dataHoraInicio'],df['keywords'], df['tipoDiscurso'], df['transcricao']], axis = 1, ignore_index=False)  
    elif tag == 'id':
        df = json_normalize(json_data['dados'], sep = ';')
        return df['id']
    elif tag == 'sumario':
        df = json_normalize(json_data['dados'], sep = ';')
        df = df.drop(labels=['uri', 'uriPartido', 'urlFoto', 'email'], axis=1, inplace=False, errors='raise')
        return df
    else:
        return None

def request(url:str, params: Optional[dict] = None) -> json:
    for i in range(MAX_RETRIES):
        try:
            if params is None:
                return requests.get(url, headers = {'Accept':'application/json'}, timeout=10).json()
            else:
                return requests.get(url, params, headers = {'Accept':'application/json'}, timeout=10).json()
        except requests.exceptions.ConnectionError:
            print("Falha de comunicação com a API")
        except requests.exceptions.Timeout:
            print("Timeout")
        except requests.exceptions.RequestException as e:
            raise SystemExit(e)    
        time.sleep(WAIT_SECONDS)
    else: print("Todas as tentativas falharam")

def request_nextPage(data: json) -> str:
    if data['links'][1]['rel'] == 'next':
        return data['links'][1]['href']
    else: return None

def discursos(id: int, legislatura: int) -> json:

    params = {
        'idLegislatura': legislatura,
        'dataInicio' : None,
        'dataFim' : None,
        'ordenarPor': 'dataHoraInicio',
        'ordem': 'asc',
        'itens' : 99,
        'pagina': 1
    }

    
    url = f'https://dadosabertos.camara.leg.br/api/v2/deputados/{id}/discursos'
    
    discursos_save(url, params, id)

    return 1

def discursos_save(url, params, id):
    for i in range(MAX_RETRIES):
        #try:
            res = request(url, params)
            discursos = json_to_dataFrame(res, 'discursos')
            if not discursos.empty:
                while request_nextPage(res) is not None:
                    res = request(request_nextPage(res))
                    discursos = pd.concat([discursos, json_to_dataFrame(res, 'discursos')], axis=0)
            path = os.path.join(PATH_DISCURSOS, str(params['idLegislatura']), str(id)+'.csv')
            discursos.to_csv(path, sep = ';', encoding='utf-8', index=False)
            break
        #except:
            #print(f'Erro ao salvar discurso do deputado de id {id}')
    #else: print("Todas as tentativas falharam")
    return 1

def deputados(legislatura: int) -> json:
    params = {
        'idLegislatura': legislatura
    }

    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

    return request(url, params)

def deputados_idList(res: json) -> pd.DataFrame:
    return json_to_dataFrame(res, 'id')

def sumario(res: json, legislatura):
    try:
        sumario = json_to_dataFrame(res, 'sumario')
        sumario.columns = ['id', 'Nome', 'Partido', 'Estado', 'Legislatura']
        #sumario = sumario.reindex(columns = sumario.columns.tolist() + ['Profissão','Quantidade discursos', 'Data/Hora', 'Checksum'])
        sumario.to_csv(os.path.join(PATH_SUMARIOS, str(legislatura)+'.csv'), sep = ';', encoding='utf-8', index=False)
    except:
        print('Erro ao criar sumário de deputados')
    else:    
        print('Sumário de deputados criado com sucesso')

def file_exist(path: str):
    return os.path.isfile(path)

def main():
    print('Digite o número da legislatura:')
    legislatura = int(sys.stdin.readline())
    
    makeDir(os.path.join(PATH_DISCURSOS, str(legislatura))) #Cria pasta para discursos
    makeDir(os.path.join(PATH_SUMARIOS))                    #Cria pasta para sumários
    
    dep = deputados(legislatura)    #Solicita a lista de deputados para a legislatura
    sumario(dep, legislatura)       #Trata e salva as informações dos deputados em um arquivo csv
    id = deputados_idList(dep)      #Criar uma lista com as ids de cada deputado

    print('Baixando discursos...')

    for i in id:
        if not file_exist(os.path.join(PATH_DISCURSOS, str(legislatura), str(i)+'.csv')):
            discursos(i, legislatura)          
    
    print('Discursos baixados com sucesso')

if __name__ == "__main__":
    main()
    