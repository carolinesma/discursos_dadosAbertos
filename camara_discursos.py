import requests
import json
import os
import errno
import csv
import pandas as pd
from pandas import json_normalize
from typing import Optional

SETUP = {'legislatura': 56, 'path_discursos': 'Teste', 'path_sumario': 'Sumários'}

def makeDir(path) -> None:
    try:
        os.makedirs(path, exist_ok = True)
    except OSError as exc:
        if exc.errno != errno.EEXIST:
            raise
        pass

def request(url:str, params: Optional[dict] = None) -> json:
    if params is None:
        return requests.get(url, headers = {'Accept':'application/json'}).json()
    else:
        return requests.get(url, params, headers = {'Accept':'application/json'}).json()
        
def next_page(data: json) -> str:
    if data['links'][1]['rel'] == 'next':
        return data['links'][1]['href']
    else: return None

def json_to_dataFrame(json_data: json, tag: str) -> pd.DataFrame:
    if tag == 'discursos':
        df = json_normalize(json_data['dados'], sep = ';')
        return df['transcricao']
    elif tag == 'id':
        df = json_normalize(json_data['dados'], sep = ';')
        return df['id']
    elif tag == 'sumario':
        df = json_normalize(json_data['dados'], sep = ';')
        df = df.drop(labels=['uri', 'uriPartido', 'idLegislatura', 'urlFoto', 'email'], axis=1, inplace=False, errors='raise')
        print (df)
        return df
    else:
        return None

def salvar_discurso(url, params, id):
    res = request(url, params)
    discursos = json_to_dataFrame(res, 'discursos')
    while next_page(res) is not None:
        res = request(next_page(res))
        discursos = pd.concat([discursos, json_to_dataFrame(res, 'discursos')], axis=0)
    discursos_csv = discursos.to_csv(os.path.join(SETUP['path_discursos'], str(SETUP['legislatura']), str(id)+'.csv'), sep = ';', encoding='utf-8', index=False)
    return 1

def lista_id_deputados(res: json) -> pd.DataFrame:
    return json_to_dataFrame(res, 'id')

def deputados(legislatura: int) -> json:
    params = {
        'idLegislatura': legislatura
    }

    url = 'https://dadosabertos.camara.leg.br/api/v2/deputados'

    return request(url, params)

def deputados_sumario(res: json):
    try:
        sumario = json_to_dataFrame(res, 'sumario')
        sumario.columns = ['Id', 'Nome', 'Partido', 'Estado']
        sumario = sumario.reindex(columns = sumario.columns.tolist() + ['Profissão','Quantidade discursos', 'checksum'])
        sumario_csv = sumario.to_csv(os.path.join(SETUP['path_sumario'], str(SETUP['legislatura'])+'.csv'), sep = ';', encoding='utf-8', index=False)
    except:
        print('Erro ao criar sumário de deputados')
    else:    
        print('Sumário de deputados criado com sucesso')

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
    
    salvar_discurso(url, params, id)

    return 1

def saveJsonFile(data, fileName):
    with open(fileName+'.json', 'w', encoding="utf-8") as fp:
        json.dump(data, fp, indent = 4, ensure_ascii=False)

def main():
    makeDir(os.path.join(SETUP['path_discursos'], str(SETUP['legislatura'])))
    makeDir(os.path.join(SETUP['path_sumario']))
    dep = deputados(SETUP['legislatura'])
    deputados_sumario(dep)
    id = lista_id_deputados(dep)
    
    for i in id:
        discursos(i, SETUP['legislatura'])

    #id = 178970
    #discursos(id, DADOS['legislatura'])
    #saveJsonFile(discursos(id, DADOS['legislatura']), "Exemplo_Dados_Brutos")

if __name__ == "__main__":
    main()
    