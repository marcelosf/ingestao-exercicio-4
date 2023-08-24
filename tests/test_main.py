import os
from .. import main
from pyspark.sql import SparkSession


BASE_DIR = os.getcwd()


def test_get_spark_session():
    session = main.get_spark_session()
    assert isinstance(session, SparkSession)


def test_read_csv():
    path = os.path.join(BASE_DIR, "sources/Bancos")
    session = main.get_spark_session()
    content = main.read_source(session, path, delimiter="\t")
    expect = ["Segmento", "CNPJ", "Nome"]
    assert content.columns == expect


def test_find_name():
    name1 = "CENTRAL DE COOPERATIVAS DE ECONOMIA E CR�DITO M�TUO DO ESTADO DO RIO GRANDE DO SUL LTDA - UNICRED CENTRAL RS"
    name2 = "CENTRAL DE COOPERATIVAS DE ECONOMIA E CRÉDITO MÚTUO DO ESTADO DO RIO GRANDE DO SUL LTDA - UNICRED CENTRAL RS"
    assert main.match(name1, name2)


def test_extract_bancos_data():
    bancos_data = main.extract_bancos_data()
    assert bancos_data.count() == 2948


def test_extract_reclamacoes_data():
    reclamacoes_data = main.extract_reclamacoes_data()
    assert reclamacoes_data.count() == 1023


def test_extract_empregados_data():
    empregados_data = main.extract_empregados_data()
    assert empregados_data.count() == 39
    