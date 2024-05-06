# Databricks notebook source
pip install pyspark textblob

# COMMAND ----------

# import re
# import nltk
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType
# from nltk.tokenize import word_tokenize
# from nltk.corpus import stopwords
# from textblob import TextBlob

# # S'assurer que nltk est téléchargé dans un contexte Spark
# nltk.download('punkt')
# nltk.download('stopwords')

# COMMAND ----------

# def clean_tweet(tweet):
#     tweet = re.sub(r'http\S+', '', tweet)  # Supprimer les URL
#     tweet = re.sub(r'@\w+', '', tweet)     # Supprimer les mentions
#     tweet = re.sub(r'#\w+', '', tweet)     # Supprimer les hashtags
#     tweet = re.sub(r'[^\w\s]', '', tweet)  # Supprimer les caractères spéciaux
#     return tweet

# def tokenize_tweet(tweet):
#     return word_tokenize(tweet)

# def remove_stopwords(tokens):
#     stop_words = set(stopwords.words('french'))
#     return [token for token in tokens if token.lower() not in stop_words]

# # Création de la session Spark
# spark = SparkSession.builder \
#     .appName("Préparation des données") \
#     .getOrCreate()
    
# tweets_df = spark.read.table("tweets")

# # Application des fonctions de nettoyage et de prétraitement
# clean_tweet_udf = udf(clean_tweet, StringType())
# tokenize_tweet_udf = udf(tokenize_tweet, StringType())
# remove_stopwords_udf = udf(remove_stopwords, StringType())

# tweets_df = tweets_df.withColumn("tweet_nettoyé", clean_tweet_udf("tweet"))
# tweets_df = tweets_df.withColumn("tweet_tokenisé", tokenize_tweet_udf("tweet_nettoyé"))
# tweets_df = tweets_df.withColumn("token_filtré", remove_stopwords_udf("tweet_tokenisé"))

# # Écrire les données dans une table Delta
# tweets_df.write.mode("overwrite").format("delta").saveAsTable("tweets_prepares")

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
cleaned_tweets_df = tweets_df.withColumn("tweet_nettoyé", regexp_replace(tweets_df["tweet"], "http\\S+|@\\w+|#[^\\s]+|[^\w\s]", ""))

# Tokenisation des tweets
tokenizer = Tokenizer(inputCol="tweet_nettoyé", outputCol="tweet_tokenisé")
tokenized_tweets_df = tokenizer.transform(cleaned_tweets_df)

# Suppression des stopwords
remover = StopWordsRemover(inputCol="tweet_tokenisé", outputCol="token_filtré", stopWords=StopWordsRemover.loadDefaultStopWords("french"))
preprocessed_tweets_df = remover.transform(tokenized_tweets_df)

# Écriture des données dans une table Delta
preprocessed_tweets_df.write.mode("overwrite").format("delta").saveAsTable("tweets_prepares")

