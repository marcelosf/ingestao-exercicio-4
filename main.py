from pyspark.sql import SparkSession


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def read_source(session: SparkSession, path: str, delimiter: str):
    df = session.read.csv(path=path, header=True, sep=delimiter)
    return df
