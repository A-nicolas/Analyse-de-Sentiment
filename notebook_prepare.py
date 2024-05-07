# Databricks notebook source
pip install pyspark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import regexp_replace

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Préparation des données") \
    .getOrCreate()

# Lecture des données
tweets_df = spark.read.table("tweets")

# Nettoyage des tweets
cleaned_tweets_df = tweets_df.withColumn("tweet_nettoyé", regexp_replace(tweets_df["tweet"], r"http\S+|@\w+|#\w+|[^\w\sÀ-ÿ]", ""))

# Tokenisation des tweets
tokenizer = Tokenizer(inputCol="tweet_nettoyé", outputCol="tweet_tokenisé")
tokenized_tweets_df = tokenizer.transform(cleaned_tweets_df)

# Suppression des stopwords
remover = StopWordsRemover(inputCol="tweet_tokenisé", outputCol="token_filtré", stopWords=StopWordsRemover.loadDefaultStopWords("french"))
preprocessed_tweets_df = remover.transform(tokenized_tweets_df)

# Écriture des données dans une table Delta
preprocessed_tweets_df.write.mode("overwrite").format("delta").saveAsTable("tweets_prepares")

display(preprocessed_tweets_df)

# COMMAND ----------

# DBTITLE 1,Démo data
tweets_df_demo = spark.read.table("tweets_prepares")
display(tweets_df_demo)
