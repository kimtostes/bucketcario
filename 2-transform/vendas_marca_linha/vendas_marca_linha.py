#!/usr/bin/python

from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, date_format

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('vendas_marca_linha') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'bucketcario1')

vendas_marca_linha = spark.read.format("bigquery").option('table', 'grupo-boticario-305900.raw_grupo_boticario.vendas').load()

vendas_marca_linha = vendas_marca_linha.dropDuplicates()

vendas_marca_linha = vendas_marca_linha.select(F.col("MARCA"), F.col("ID_LINHA").cast("long"), F.col("LINHA"), F.col("QTD_VENDA").cast("long"))
vendas_marca_linha = vendas_marca_linha.groupBy("MARCA", "ID_LINHA", "LINHA").sum("QTD_VENDA").alias("QTD_VENDA").sort(F.col("LINHA").asc())
vendas_marca_linha = vendas_marca_linha.select(F.col("MARCA"), F.col("ID_LINHA"), F.col("LINHA"), F.col("sum(QTD_VENDA)").alias("QTD_VENDA"))

vendas_marca_linha.printSchema()
vendas_marca_linha.show()

vendas_marca_linha.write.format("bigquery") \
    .mode('append')\
    .option('table', 'grupo-boticario-305900.dataset_grupo_boticario.vendas_marca_linha') \
    .save()
