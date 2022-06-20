#coding:utf-8
# ler arquivos vários arquivos *.csv da lading via abfs
# Lendo todos os arquivos .csv (>4GB)

from os.path import abspath
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# setup da aplicação Spark
spark = SparkSession \
    .builder \
    .appName("job-1-spark-hdinsight") \
    .config("spark.sql.warehouse.dir", abspath('spark-warehouse')) \
    .getOrCreate()

# definindo o método de logging da aplicação use INFO somente para DEV [INFO,ERROR]
spark.sparkContext.setLogLevel("ERROR")

# lendo os dados do Data Lake
# abfs://<file_system>@<account_name>.dfs.core.windows.net/<path>/<file_name>
# file_system = landing
# account_name = edzstorage.dfs.core.windows.net
# path = /*.csv

df = spark.read.format("csv")\
    .option("header", "True")\
    .option("inferSchema","True")\
    .csv("abfs://landing@edzstorage.dfs.core.windows.net/*.csv")

# imprime os dados lidos da raw
print ("\nImprime os dados lidos da lading:")
print (df.show())

# imprime o schema do dataframe
print ("\nImprime o schema do dataframe lido da raw:")
print (df.printSchema())

# converte para formato parquet
print ("\nEscrevendo os dados lidos da raw para parquet na processing zone...")
df.write.format("parquet")\
        .mode("overwrite")\
        .save("abfs://processing@edzstorage.dfs.core.windows.net/df-parquet-file.parquet")

# lendo arquivos parquet
df_parquet = spark.read.format("parquet")\
    .load("abfs://processing@edzstorage.dfs.core.windows.net/df-parquet-file.parquet")

# imprime os dados lidos em parquet
print ("\nImprime os dados lidos em parquet da processing zone")
print (df_parquet.show())

# cria uma view para trabalhar com sql
df_parquet.createOrReplaceTempView("view_df_parquet")

# processa os dados conforme regra de negócio
df_result = spark.sql("SELECT BNF_CODE as Bnf_code \
                        ,SUM(ACT_COST) as Soma_Act_cost \
                        ,SUM(QUANTITY) as Soma_Quantity \
                        ,SUM(ITEMS) as Soma_items \
                        ,AVG(ACT_COST) as Media_Act_cost \
                       FROM view_df_parquet \
                       GROUP BY bnf_code")

# imprime o resultado do dataframe criado
print ("\n ========= Imprime o resultado do dataframe processado =========\n")
print (df_result.show())

# converte para formato parquet
print ("\nEscrevendo os dados processados na Curated Zone...")

# converte os dados processados para parquet e escreve na curated zone
df_result.write.format("parquet")\
          .mode("overwrite")\
          .save("abfs://curated@edzstorage.dfs.core.windows.net/df-result-file.parquet")

# # para a aplicação
spark.stop()
