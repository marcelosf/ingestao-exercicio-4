import os
from typing import List
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import split, col, trim, sum, udf
from decouple import config


SOURCES_DIR = os.path.join(os.getcwd(), "sources")
OUTPUT = os.path.join(os.getcwd(), "output")

PREPROCESS_BANCO_PATH = config("PREPROCESS_BANCO_PATH")
PREPROCESS_EMPREGADOS_PATH = config("PREPROCESS_EMPREGADOS_PATH")
PREPROCESS_RECLAMACOES_PATH = config("PREPROCESS_RECLAMACOES_PATH")

AVALIABLE_DATA = config("AVALIABLE_DATA")

COLUMNS_FROM_EMPREGADOS = ["Nome", "Geral", "Remuneração e benefícios"]
COLUMNS_FROM_RECLAMACOES = ["Instituição financeira", "Índice", "Quantidade total de reclamações", "Quantidade de clientes  SCR"]

COLUMNS_EMPREGADOS_RENAME = {
    "Nome": "nome",
    "Geral": "satisfacao_funcionario",
    "Remuneração e benefícios": "indice_remuneracao"
}

COLUMNS_BANCOS_RENAME = {
    "Segmento": "segmento",
    "CNPJ": "cnpj",
    "Nome": "nome"
}

COLUMNS_RECLAMACOES_RENAME = {
    "Instituição financeira": "nome",
    "Índice": "indice_reclamacoes",
    "Quantidade total de reclamações": "quantidade_reclamacoes",
    "Quantidade de clientes  SCR": "quantidade_clientes"
}


def get_spark_session():
    spark = SparkSession.builder.getOrCreate()
    return spark


def read_source(session: SparkSession, path: str, delimiter: str, encoding=None, schema=None):
    df = session.read.csv(path=path, header=True, sep=delimiter, encoding=encoding, schema=schema)
    return df


def match(name1: str, name2: str) -> bool:
    breakpoint()
    parts = name1.split("�")
    part_to_compare = parts[-1]

    if not part_to_compare:
        part_to_compare = parts[0]

    return part_to_compare in name2


def extract_data(path: str, delimiter: str, session, encoding=None, schema=None):
    path = os.path.join(SOURCES_DIR, path)
    data = read_source(session, path, delimiter=delimiter, encoding=encoding, schema=schema)
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


def aggregate(df: DataFrame, by: str,  columns_to_sum: list):
    args = [f'sum("{item}").alias("{item}")' for item in columns_to_sum]
    args = ", ".join(args)
    statement = f'result = df.groupBy("{by}").agg({args})'
    local_variables = {"df": df}
    exec(statement, globals(), local_variables)
    return local_variables["result"]


def join_tables(main_table: DataFrame, tables_to_join: List[DataFrame]) -> DataFrame:
    for table, column in tables_to_join:
        main_table = main_table.join(table, column, "outer")
    return main_table


def convert_column_data_type(df: DataFrame, columns_type: List[tuple]) -> DataFrame:
    for column in columns_type:
        column_name, column_type = column
        df = df.withColumn(column_name, getattr(df, column_name).cast(column_type))
    return df


def write_to_parquet(df: DataFrame, path):
    df.write.mode("overwrite").parquet(os.path.join(OUTPUT, path))


def preprocess_bancos_data(session):
    bancos_data = extract_bancos_data(session)
    bancos_data = clean_banco_name(bancos_data)
    bancos_data = rename_columns(bancos_data, COLUMNS_BANCOS_RENAME)
    bancos_data = remove_spaces(df=bancos_data, column="nome")
    columns_type = [("segmento", "string"), ("cnpj", "string"), ("nome", "string")]
    bancos_data = convert_column_data_type(df=bancos_data, columns_type=columns_type)
    write_to_parquet(df=bancos_data, path=PREPROCESS_BANCO_PATH)


def preprocess_empregados_data(session):
    empregados_data = extract_empregados_data(session)
    empregados_data = filter_columns(df=empregados_data, columns=COLUMNS_FROM_EMPREGADOS)
    empregados_data = rename_columns(df=empregados_data, columns_to_rename=COLUMNS_EMPREGADOS_RENAME)
    columns_type = [("nome", "string"), ("satisfacao_funcionario", "float"), ("indice_remuneracao", "float")]
    empregados_data = convert_column_data_type(df=empregados_data, columns_type=columns_type)
    empregados_data = aggregate(df=empregados_data, by="nome", columns_to_sum=["satisfacao_funcionario", "indice_remuneracao"])
    write_to_parquet(df=empregados_data, path=PREPROCESS_EMPREGADOS_PATH)


def preprocess_reclamacoes_data(session):
    reclamacoes_data = extract_reclamacoes_data(session)
    reclamacoes_data = filter_columns(df=reclamacoes_data, columns=COLUMNS_FROM_RECLAMACOES)
    reclamacoes_data = rename_columns(df=reclamacoes_data, columns_to_rename=COLUMNS_RECLAMACOES_RENAME)
    columns_type = [("nome", "string"), ("indice_reclamacoes", "float"), ("quantidade_reclamacoes", "integer"), ("quantidade_clientes", "integer")]
    reclamacoes_data = convert_column_data_type(df=reclamacoes_data, columns_type=columns_type)
    reclamacoes_data = aggregate(df=reclamacoes_data, by="nome", columns_to_sum=["quantidade_reclamacoes", "quantidade_clientes"])
    write_to_parquet(df=reclamacoes_data, path=PREPROCESS_RECLAMACOES_PATH)


def merge_tables():
    session = get_spark_session()
    session.udf.register("match_data", match, BooleanType())

    bancos_df = session.read.parquet(os.path.join(OUTPUT, PREPROCESS_BANCO_PATH))
    empregados_df = session.read.parquet(os.path.join(OUTPUT, PREPROCESS_EMPREGADOS_PATH))

    bancos_df.join(empregados_df, (match(bancos_df.nome, empregados_df.nome) == True)).show()

    write_to_parquet(bancos_df, AVALIABLE_DATA)


def run():
    session = get_spark_session()
    preprocess_bancos_data(session)
    preprocess_empregados_data(session)
    preprocess_reclamacoes_data(session)
    merge_tables()


if __name__ == "__main__":
    run()

    # TODO use user defined function UDF: 
    #  https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html