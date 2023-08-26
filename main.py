import os
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col


SOURCES_DIR = os.path.join(os.getcwd(), "sources")


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def read_source(session: SparkSession, path: str, delimiter: str, encoding=None):
    df = session.read.csv(path=path, header=True, sep=delimiter, encoding=encoding)
    return df


def match(name1: str, name2: str):
    parts = name1.split("�")
    part_to_compare = parts[-1]

    if not part_to_compare:
        part_to_compare = parts[0]

    return part_to_compare in name2


def extract_data(path: str, delimiter: str, encoding=None):
    session = get_spark_session()
    path = os.path.join(SOURCES_DIR, path)
    data = read_source(session, path, delimiter=delimiter, encoding=encoding)
    return data    


def extract_bancos_data():
    return extract_data(path="Bancos", delimiter="\t")


def extract_reclamacoes_data():
    return extract_data(path="Reclamacoes", delimiter=";", encoding="ISO-8859-1")


def extract_empregados_data():
    return extract_data(path="Empregados", delimiter="|")


def rename_columns(df: DataFrame, columns_to_rename: list):
    reclamacoes_with_columns_renamed = df.withColumnsRenamed(columns_to_rename)
    return reclamacoes_with_columns_renamed


def filter_columns(df: DataFrame, columns: list):
    selected_columns = df.select(columns)
    return selected_columns


def clean_banco_name(df: DataFrame):
    cleaned_df = df.select(df.Segmento, df.CNPJ, split(col("Nome"), "-")[0].alias("Nome"))
    return cleaned_df
