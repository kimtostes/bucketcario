#!/usr/bin/python

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from google.cloud import bigquery

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('grupo-boticario') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set('temporaryGcsBucket', "bucketcario1")

input_data = spark.read.option("header",True).option("delimiter", ";").csv("gs://bucketcario1/*.csv")
input_data = input_data.select(F.col("ID_MARCA"), F.col("MARCA"), F.col("ID_LINHA"), F.col("LINHA"), F.col("DATA_VENDA"), F.col("QTD_VENDA"))

input_data.printSchema()
input_data.show()

input_data.write.format("bigquery") \
    .mode('overwrite')\
    .option('table', 'grupo-boticario-305900.raw_grupo_boticario.vendas') \
    .save()