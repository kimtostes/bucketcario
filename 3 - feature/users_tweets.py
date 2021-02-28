#!/usr/bin/python

from pyspark.sql import SparkSession
from google.cloud import bigquery
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, DateType
import tweepy as tw

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('tweets') \
  .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar') \
  .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'bucketcario1')

max_vendas_linha = spark.read.format("bigquery").option('table', 'grupo-boticario-305900.dataset_grupo_boticario.vendas_linha_ano_mes').load()
max_vendas_linha = max_vendas_linha.select(F.col("DATA_VENDA"), F.col("LINHA"), F.col("QTD_VENDA")).filter(F.col("DATA_VENDA") == 201912)
max_vendas_linha = max_vendas_linha.groupby("LINHA").max("qtd_venda").sort(F.col("max(QTD_VENDA)").desc())
max_vendas_linha = max_vendas_linha.collect()[0][0]
max_vendas_linha

api_key = '1FOu2Z3gsq3SRi8FzVhIpe53s'
api_secret_key = 'yS7AcSzqld0qjDe9wKOXLmnyKoJdcQ51hh6aMhBjepeQUIzIqU'
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAGt%2BNAEAAAAAOmKA0%2FR%2BwqeY1NK9MDiK00ghSIE%3D5JlkS8BOcquS346AxPA3Ivq13sGtQENqaB0d5X4fuljoTSI11w'
access_token = '1365083166306885633-J8ShuPVsBrnQSyo0j8puk9jaE02U1Q'
access_token_secret = 'ItN4TuqnPLKvhUQJICFQzaaIPoVFphCicKdvzzLgzU6P7'

auth = tw.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)

busca = "Boticário+" + max_vendas_linha +  " -filter:tweets"
print(busca)
tweets = tw.Cursor(api.search, q=busca, lang="pt").items(50)

tweets = [(tweet.created_at, tweet.user.screen_name, tweet.text) for tweet in tweets]

#print(tweets[:50])

schema = StructType([
    StructField('created_at', DateType(), True),
    StructField('user_screen_name', StringType(), True),
    StructField('tweet', StringType(), True)
])

rdd = spark.sparkContext.parallelize(tweets)

users_tweets = spark.createDataFrame(rdd,schema)

users_tweets.printSchema()

users_tweets = users_tweets.select(F.col("user_screen_name"), F.col("tweet"))

users_tweets.printSchema()

users_tweets.show()

users_tweets.write.format("bigquery") \
    .mode('append')\
    .option('table', 'grupo-boticario-305900.dataset_grupo_boticario.users_tweets') \
    .save()