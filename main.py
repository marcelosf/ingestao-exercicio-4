import os
from pyspark.sql import SparkSession


SOURCES_DIR = os.path.join(os.getcwd(), "sources")


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def read_source(session: SparkSession, path: str, delimiter: str):
    df = session.read.csv(path=path, header=True, sep=delimiter)
    return df


def match(name1: str, name2: str):
    parts = name1.split("�")
    part_to_compare = parts[-1]

    if not part_to_compare:
        part_to_compare = parts[0]

    return part_to_compare in name2


def extract_data(path: str, delimiter: str):
    session = get_spark_session()
    path = os.path.join(SOURCES_DIR, path)
    data = read_source(session, path, delimiter=delimiter)
    return data    


def extract_bancos_data():
    return extract_data(path="Bancos", delimiter="\t")


def extract_reclamacoes_data():
    return extract_data(path="Reclamacoes", delimiter=";")


def extract_empregados_data():
    return extract_data(path="Empregados", delimiter="|")

