# Databricks notebook source
pip install tweepy nltk pyspark textblob

# COMMAND ----------

import re
import tweepy
import nltk
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from textblob import TextBlob
nltk.download('punkt')
nltk.download('stopwords')
