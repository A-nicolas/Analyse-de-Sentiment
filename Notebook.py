# Databricks notebook source
pip install tweepy nltk pyspark textblob

# COMMAND ----------

import tweepy
import re
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from textblob import TextBlob
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
import nltk
nltk.download('punkt')
nltk.download('stopwords')

# COMMAND ----------

client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAABHvtQEAAAAAZYZuSyP3Xt5GNPVE8SDaFL8km%2FE%3DHWxI0NcvZzQYB28eJummwmZ8zfb20lLBMHkpzERg4oLF9tFktq")

# COMMAND ----------

query = "Vinicius lang:fr -is:retweet -is:reply"
max_results = 10

tweets_fr = client.search_recent_tweets(query=query, tweet_fields=['text'], max_results=max_results)

# COMMAND ----------

# Nettoyage des tweets
def clean_tweet(tweet):
    tweet = re.sub(r'http\S+', '', tweet)  # Supprimer les URL
    tweet = re.sub(r'@\w+', '', tweet)     # Supprimer les mentions
    tweet = re.sub(r'#\w+', '', tweet)     # Supprimer les hashtags
    tweet = re.sub(r'[^\w\s]', '', tweet)  # Supprimer les caractères spéciaux
    return tweet

# Tokenisation
def tokenize_tweet(tweet):
    return word_tokenize(tweet)

# Filtration des stop-words : mots inutiles
def remove_stopwords(tokens):
    stop_words = set(stopwords.words('french'))
    return [token for token in tokens if token.lower() not in stop_words]

# Fonction d'analyse de sentiment avec TextBlob
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
#tweets_with_sentiment.show()
display(tweets_with_sentiment)

