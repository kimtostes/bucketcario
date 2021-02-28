#!/usr/bin/python

from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, date_format

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('vendas_ano_mes') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set('temporaryGcsBucket', "bucketcario1")

vendas_ano_mes = spark.read.format("bigquery").option('table', 'grupo-boticario-305900.raw_grupo_boticario.vendas').load()

vendas_ano_mes = vendas_ano_mes.select(F.col("DATA_VENDA"), F.col("QTD_VENDA").cast("long")).withColumn("DATA_VENDA", date_format(to_date(F.col("DATA_VENDA"),"dd/MM/yyyy"), "yyyyMM"))

vendas_ano_mes = vendas_ano_mes.groupBy("DATA_VENDA").sum("QTD_VENDA").alias("QTD_VENDA").sort(F.col("DATA_VENDA").desc())

vendas_ano_mes = vendas_ano_mes.select(F.col("DATA_VENDA").cast("long"), F.col("sum(QTD_VENDA)").cast("long").alias("QTD_VENDA"))

vendas_ano_mes.printSchema()
vendas_ano_mes.show()

vendas_ano_mes.write.format("bigquery") \
    .mode('append')\
    .option('table', 'grupo-boticario-305900.dataset_grupo_boticario.vendas_ano_mes') \
    .save()
