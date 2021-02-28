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
vendas_ano_mes = spark.read.format("bigquery") \
  .option('table', table) \
  .load()

vendas_ano_mes = \
vendas_ano_mes.select(F.col("DATA_VENDA"), F.col("QTD_VENDA").cast("long")).withColumn(
    "DATA_VENDA", date_format(to_date(F.col("DATA_VENDA"),"dd/MM/yyyy"), "yyyyMM"))
vendas_ano_mes.printSchema()

vendas_ano_mes = vendas_ano_mes.groupBy("DATA_VENDA").sum("QTD_VENDA").alias("QTD_VENDA").sort(F.col("DATA_VENDA").desc())

vendas_ano_mes = vendas_ano_mes.select(F.col("DATA_VENDA").cast("long"), F.col("sum(QTD_VENDA)").cast("long").alias("QTD_VENDA"))

vendas_ano_mes.show()

vendas_ano_mes.write.format("bigquery") \
    .mode('append')\
    .option('table', 'grupo-boticario-305900.dataset_grupo_boticario.vendas_ano_mes') \
    .save()