# Databricks notebook source
pip install pyspark textblob

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob

# COMMAND ----------


def analyze_sentiment(tweet):
    blob = TextBlob(tweet)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return "positif"
    elif sentiment < 0:
        return "négatif"
    else:
        return "neutre"

# COMMAND ----------

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Analyse de sentiments") \
    .getOrCreate()

# Analyse de sentiments avec TextBlob
sentiment_udf = udf(analyze_sentiment, StringType())
preprocessed_tweets_data_df = spark.read.table("tweets_prepares")
tweets_with_sentiment = preprocessed_tweets_data_df.withColumn("sentiment", sentiment_udf("tweet_nettoyé"))

# Supprimer les doublons avant d'insérer dans la table
unique_tweets_with_sentiment = tweets_with_sentiment.dropDuplicates(["tweet_nettoyé"])

#Insertion des données dans une table pour rapport POWER BI
unique_tweets_with_sentiment.write.mode("overwrite").saveAsTable("analyse_de_sentiments")

# Affichage des résultats
display(unique_tweets_with_sentiment)
