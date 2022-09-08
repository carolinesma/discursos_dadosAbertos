# Aquisição de dados abertos de discursos de políticos

O script camara_discursos.py tem o propósito de fazer download e tratamento dos dados de discursos e informações de deputados fornecidos pela API de dados abertos da câmara dos deputados <https://dadosabertos.camara.leg.br/swagger/api.html>

Os discursos são baixados em conjunto de acordo com a legislatura. Por exemplo, se for inserido, quando requisitado, o número a legislatura 56 (2019-2022) serão baixados todos os discursos disponíveis de todos os deputados em execício nesse período.

As informações dos discursos são salvas em arquivos csv nomeados com o número de de legislatura de cada deputado
