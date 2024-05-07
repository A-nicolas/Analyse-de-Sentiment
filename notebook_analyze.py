# Databricks notebook source
pip install pyspark textblob textblob-fr

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob
from textblob_fr import PatternTagger, PatternAnalyzer

# COMMAND ----------


def analyze_sentiment(tweet):
    blob = TextBlob(tweet, pos_tagger=PatternTagger(), analyzer=PatternAnalyzer())
    sentiment = blob.sentiment[0]
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

#Insertion des données dans une table pour rapport POWER BI
tweets_with_sentiment.write.mode("overwrite").saveAsTable("analyse_de_sentiments")

# Affichage des résultats
display(tweets_with_sentiment)

# COMMAND ----------

# DBTITLE 1,Démo data analyze
tweets_df_demo = spark.read.table("analyse_de_sentiments")
display(tweets_df_demo)
