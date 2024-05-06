# Databricks notebook source
pip install tweepy

# COMMAND ----------

import tweepy

# COMMAND ----------

# Connexion à l'API de Twitter
client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAABHvtQEAAAAAZYZuSyP3Xt5GNPVE8SDaFL8km%2FE%3DHWxI0NcvZzQYB28eJummwmZ8zfb20lLBMHkpzERg4oLF9tFktq")

query = "Zhegrova lang:fr -is:retweet -is:reply"

tweets_fr = client.search_recent_tweets(query=query, tweet_fields=['text'], max_results=20)

# Chargement des tweets dans un DataFrame Spark
tweets_data = [(tweet.text,) for tweet in tweets_fr.data]
tweets_df = spark.createDataFrame(tweets_data, ["tweet"])

# Écrire les données dans une table Delta
tweets_df.write.mode("overwrite").format("delta").saveAsTable("tweets")
