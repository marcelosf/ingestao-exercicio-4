import os
from .. import main
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, FloatType, StringType


from ..schemas import reclamacoes_schema


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


def test_extract_bancos_data(session):
    bancos_data = main.extract_bancos_data(session)
    assert bancos_data.count() == 2948


def test_extract_reclamacoes_data(session):
    reclamacoes_data = main.extract_reclamacoes_data(session)
    assert reclamacoes_data.count() == 918


def test_extract_empregados_data(session):
    empregados_data = main.extract_empregados_data(session)
    assert empregados_data.count() == 39


def test_rename_columns(session):
    reclamacoes_data = main.extract_reclamacoes_data(session)
    columns_to_rename = {
        "Quantidade de clientes \x96 SCR": "quantidade_clientes",
        "Quantidade total de reclamações": "quantidade_reclamacoes"
    }
    reclamacoes_with_columns_renamed = main.rename_columns(reclamacoes_data, columns_to_rename)
    assert 'quantidade_clientes' in reclamacoes_with_columns_renamed.columns
    assert 'quantidade_reclamacoes' in reclamacoes_with_columns_renamed.columns


def test_filter_columns(session):
    reclamacoes_data = main.extract_reclamacoes_data(session)
    columns_to_rename = {"Quantidade de clientes \x96 SCR": "quantidade_clientes", "Quantidade total de reclamações": "quantidade_reclamacoes"}
    reclamacoes_with_columns_renamed = main.rename_columns(reclamacoes_data, columns_to_rename)
    expect = ["quantidade_clientes", "quantidade_reclamacoes"]
    reclamacoes_columns = main.filter_columns(reclamacoes_with_columns_renamed, expect)
    assert reclamacoes_columns.columns == expect


def test_clean_banco_name(session):
    bancos_data = main.extract_bancos_data(session)
    cleaned_data = main.clean_banco_name(bancos_data)
    expect = "BANCO DO BRASIL"
    assert expect in cleaned_data.first()["Nome"]


def test_remove_spaces(session):
    bancos_data = main.extract_bancos_data(session)
    cleaned_data = main.clean_banco_name(bancos_data)
    cleaned_data = main.remove_spaces(cleaned_data, "Nome")
    expect = "BANCO DO BRASIL"
    assert cleaned_data.first()['Nome'] == expect


def test_group_by_name(session):
    columns = ["name", "rate"]
    data = [("banco do brasil", 5), ("banco do brasil", 10), ("itau", 4)]
    df = session.createDataFrame(data).toDF(*columns)
    grouped = main.group_by_name(df=df, colum_to_group="name", colum_to_sum="rate")
    assert grouped.first()["sum(rate)"] == 15


def test_join_tables(session):
    table_one_columns = ['nome_banco', 'cnpj', 'classificacao', 'indice_satisfacao_funcionarios']
    table_one_data = [('banco do brasil', '1111111', 's1', '80')]
    table_df = session.createDataFrame(table_one_data).toDF(*table_one_columns)

    table_two_columns = ['nome_banco', 'indice_reclamacoes']
    table_two_data = [('banco do brasil', '20')]
    table_one_df = session.createDataFrame(table_two_data).toDF(*table_two_columns)

    table_two_columns = ['nome_banco', 'indice_satisfacao_salarios']
    table_two_data = [('banco do brasil', '70')]
    table_two_df = session.createDataFrame(table_two_data).toDF(*table_two_columns)

    tables_to_join = [(table_one_df, "nome_banco"), (table_two_df, "nome_banco")]

    result = main.join_tables(table_df, tables_to_join)
    expect = [
        'nome_banco',
        'cnpj',
        'classificacao',
        'indice_satisfacao_funcionarios',
        'indice_reclamacoes',
        'indice_satisfacao_salarios'
    ]
    assert result.columns == expect


def test_aggregate(session):
    table_columns = ['nome', 'indice_reclamacoes', 'quantidade_reclamacoes', 'quantidade_clientes']
    table_data = [('banco do brasil', 20.0, 15, 10), ('banco do brasil', 20.0, 15, 10)]
    table = session.createDataFrame(table_data, reclamacoes_schema).toDF(*table_columns)
    columns_to_sum = ["indice_reclamacoes", "quantidade_reclamacoes", "quantidade_clientes"]
    result = main.aggregate(df=table, by="nome", columns_to_sum=columns_to_sum)
    assert result.first()["indice_reclamacoes"] == 40
    assert result.first()["quantidade_reclamacoes"] == 30



def test_convert_column_types(session):
    table_columns = ['nome', 'indice_reclamacoes', 'quantidade_reclamacoes', 'quantidade_clientes']
    table_data = [('banco do brasil', "20.0", "15", "10"), ('banco do brasil', "20.0", "15", "10")]
    table = session.createDataFrame(table_data).toDF(*table_columns)
    column_type = [
        ("nome", StringType()),
        ("indice_reclamacoes", FloatType()),
        ("quantidade_reclamacoes", IntegerType()),
        ("quantidade_clientes", IntegerType()),
    ]
    result = main.convert_column_data_type(df=table, columns_type=column_type)
    expect = [StringType(), FloatType(), IntegerType(), IntegerType()]
    columns = [field.dataType for field in result.schema.fields]
    assert columns == expect
