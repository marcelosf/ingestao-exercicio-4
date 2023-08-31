import os
from typing import List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, trim


SOURCES_DIR = os.path.join(os.getcwd(), "sources")
OUTPUT = os.path.join(os.getcwd(), "output")
PREPROCESS_BANCO_PATH = "preprocess/bancos/bancos.parquet"
PREPROCESS_EMPREGADOS_PATH = "preprocess/empregados/empregados.parquet"


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


def extract_data(path: str, delimiter: str, session, encoding=None):
    path = os.path.join(SOURCES_DIR, path)
    data = read_source(session, path, delimiter=delimiter, encoding=encoding)
    return data


def extract_bancos_data(session):
    return extract_data(path="Bancos", delimiter="\t", session=session)


def extract_reclamacoes_data(session):
    return extract_data(path="Reclamacoes", delimiter=";", encoding="ISO-8859-1", session=session)


def extract_empregados_data(session):
    return extract_data(path="Empregados", delimiter="|", session=session)


def rename_columns(df: DataFrame, columns_to_rename: dict):
    with_columns_renamed = df.withColumnsRenamed(columns_to_rename)
    return with_columns_renamed


def filter_columns(df: DataFrame, columns: list):
    selected_columns = df.select(columns)
    return selected_columns


def clean_banco_name(df: DataFrame):
    cleaned_df = df.select(df.Segmento, df.CNPJ, split(col("Nome"), "-")[0].alias("Nome"))
    return cleaned_df


def remove_spaces(df: DataFrame, column: str):
    column_obj = getattr(df, column)
    trimed_df = df.withColumn(column, trim(column_obj))
    return trimed_df


def group_by_name(df: DataFrame, colum_to_group: str, colum_to_sum: str):
    grouped_df = df.groupBy(colum_to_group).sum(colum_to_sum)
    return grouped_df


def join_tables(main_table: DataFrame, tables_to_join: List[DataFrame]) -> DataFrame:
    for table, column in tables_to_join:
        main_table = main_table.join(table, column, "outer")
    print(main_table.columns)
    return main_table


def write_to_parquet(df: DataFrame, path):
    df.write.mode("overwrite").parquet(os.path.join(OUTPUT, path))


def preprocess_bancos_data(session):
    bancos_data = extract_bancos_data(session)
    bancos_data = clean_banco_name(bancos_data)
    bancos_data = rename_columns(bancos_data, {"Segmento": "segmento", "CNPJ": "cnpj", "Nome": "nome"})
    bancos_data = remove_spaces(df=bancos_data, column="nome")
    write_to_parquet(df=bancos_data, path=PREPROCESS_BANCO_PATH)


def preprocess_empregados_data(session):
    empregados_data = extract_empregados_data(session)
    write_to_parquet(df=empregados_data, path=PREPROCESS_EMPREGADOS_PATH)


def run():
    session = get_spark_session()
    preprocess_bancos_data(session)
    preprocess_empregados_data(session)


if __name__ == "__main__":
    run()