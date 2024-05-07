# Databricks notebook source
pip install tweepy

# COMMAND ----------

import tweepy

# COMMAND ----------

# DBTITLE 1,Collecte des données
# Récupérer le bearer token depuis les secrets Databricks
bearer_token_secret = dbutils.secrets.get(scope="twitter-api", key="bearer_token")
bearer_token = str(bearer_token_secret)

# Connexion à l'API de Twitter
client = tweepy.Client(bearer_token=bearer_token)

query = "#MetGala lang:fr -is:retweet -is:reply"

tweets_fr = client.search_recent_tweets(query=query, tweet_fields=['text'], max_results=20)

# Chargement des tweets dans un DataFrame Spark
tweets_data = [(tweet.text,) for tweet in tweets_fr.data]
tweets_df = spark.createDataFrame(tweets_data, ["tweet"])

# Écrire les données dans une table Delta
tweets_df.write.mode("overwrite").format("delta").saveAsTable("tweets")

display(tweets_df)

# COMMAND ----------

# DBTITLE 1,Démo data ingest
tweets_df_demo = spark.read.table("tweets")
display(tweets_df_demo)
