# Databricks notebook source
pip install tweepy nltk pyspark textblob

# COMMAND ----------

# Databricks notebook source
import tweepy
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
#from fonctions import clean_tweet, tokenize_tweet, remove_stopwords, analyze_sentiment


# COMMAND ----------

client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAABHvtQEAAAAAZYZuSyP3Xt5GNPVE8SDaFL8km%2FE%3DHWxI0NcvZzQYB28eJummwmZ8zfb20lLBMHkpzERg4oLF9tFktq")

# COMMAND ----------

query = "Vinicius lang:fr -is:retweet -is:reply"
max_results = 10

tweets_fr = client.search_recent_tweets(query=query, tweet_fields=['text'], max_results=max_results)

# COMMAND ----------

# MAGIC %run ./notebook_fonctions

# COMMAND ----------

# Création de la session Spark
spark = SparkSession.builder \
    .appName("Analyse de Sentiments") \
    .getOrCreate()
    
# Chargement des tweets dans un DataFrame Spark
tweets_data = [(tweet.text,) for tweet in tweets_fr.data]
tweets_df = spark.createDataFrame(tweets_data, ["tweet"])

# Application des fonctions de nettoyage et de prétraitement
clean_tweet_udf = udf(clean_tweet, StringType())
tokenize_tweet_udf = udf(tokenize_tweet, StringType())
remove_stopwords_udf = udf(remove_stopwords, StringType())

cleaned_tweets_df = tweets_df.withColumn("tweet_nettoyé", clean_tweet_udf("tweet"))
tokenized_tweets_df = cleaned_tweets_df.withColumn("tweet_tokenisé", tokenize_tweet_udf("tweet_nettoyé"))
preprocessed_tweets_df = tokenized_tweets_df.withColumn("token_filtré", remove_stopwords_udf("tweet_tokenisé"))

# Analyse de sentiments avec TextBlob
sentiment_udf = udf(analyze_sentiment, StringType())
tweets_with_sentiment = preprocessed_tweets_df.withColumn("sentiment", sentiment_udf("tweet_nettoyé"))

# Affichage des résultats
display(tweets_with_sentiment)
tweets_with_sentiment.write.saveAsTable("analyse_de_sentiments")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM analyse_de_sentiments
