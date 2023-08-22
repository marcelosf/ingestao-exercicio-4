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

