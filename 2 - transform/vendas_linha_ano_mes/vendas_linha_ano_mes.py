#!/usr/bin/python

from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql import functions as F
from pyspark.sql.functions import col, unix_timestamp, to_date, date_format, from_unixtime,current_date
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('grupo-boticario') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

bucket = "bucketcario1"
table = 'grupo-boticario-305900.raw_grupo_boticario.raw_vendas'

spark.conf.set('temporaryGcsBucket', bucket)

vendas_linha_ano_mes = spark.read.format("bigquery") \
  .option('table', table) \
  .load()

vendas_linha_ano_mes  = vendas_linha_ano_mes.select(
    F.col("DATA_VENDA"),F.col("ID_LINHA"), F.col("LINHA"), F.col("QTD_VENDA").cast("int")
         ).withColumn(
        "DATA_VENDA", to_date(F.col("DATA_VENDA"),"dd/MM/yyyy")
    )

vendas_linha_ano_mes = vendas_linha_ano_mes.groupBy("DATA_VENDA", "ID_LINHA", "LINHA").sum("QTD_VENDA").sort(F.col("DATA_VENDA").desc())

vendas_linha_ano_mes = vendas_linha_ano_mes.select(F.col("DATA_VENDA"), F.col("ID_LINHA").cast("long"), F.col("LINHA").cast("string"), F.col("sum(QTD_VENDA)").cast("long").alias("QTD_VENDA"))

vendas_linha_ano_mes.printSchema()
vendas_linha_ano_mes.show()

vendas_linha_ano_mes.write.format("bigquery") \
    .mode('overwrite')\
    .option('table', 'grupo-boticario-305900.raw_grupo_boticario.feature_vendas_linha_ano_mes') \
    .save()