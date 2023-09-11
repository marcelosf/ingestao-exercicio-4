from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType


bancos_schema = StructType([
    StructField("segmento", StringType(), True),
    StructField("cnpj", StringType(), True),
    StructField("nome", StringType(), True)
])


empregados_schema = StructType([
    StructField("nome", StringType(), True),
    StructField("satisfacao_funcionario", FloatType(), True),
    StructField("indice_remuneracao", FloatType(), True)
])


reclamacoes_schema = StructType([
    StructField("nome", StringType(), True),
    StructField("indice_reclamacoes", FloatType(), True),
    StructField("quantidade_reclamacoes", IntegerType(), True),
    StructField("quantidade_de_clientes", IntegerType(), True)
])