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
    assert reclamacoes_data.count() == 918


def test_extract_empregados_data():
    empregados_data = main.extract_empregados_data()
    assert empregados_data.count() == 39


def test_rename_columns():
    reclamacoes_data = main.extract_reclamacoes_data()
    columns_to_rename = {
        "Quantidade de clientes \x96 SCR": "quantidade_clientes",
        "Quantidade total de reclamações": "quantidade_reclamacoes"
    }
    reclamacoes_with_columns_renamed = main.rename_columns(reclamacoes_data, columns_to_rename)
    assert 'quantidade_clientes' in reclamacoes_with_columns_renamed.columns
    assert 'quantidade_reclamacoes' in reclamacoes_with_columns_renamed.columns


def test_filter_columns():
    reclamacoes_data = main.extract_reclamacoes_data()
    columns_to_rename = {"Quantidade de clientes \x96 SCR": "quantidade_clientes", "Quantidade total de reclamações": "quantidade_reclamacoes"}
    reclamacoes_with_columns_renamed = main.rename_columns(reclamacoes_data, columns_to_rename)
    expect = ["quantidade_clientes", "quantidade_reclamacoes"]
    reclamacoes_columns = main.filter_columns(reclamacoes_with_columns_renamed, expect)
    assert reclamacoes_columns.columns == expect


def test_clean_banco_name():
    bancos_data = main.extract_bancos_data()
    cleaned_data = main.clean_banco_name(bancos_data)
    expect = "BANCO DO BRASIL"
    assert expect in cleaned_data.first()["Nome"]

