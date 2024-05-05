# Databricks notebook source
def clean_tweet(tweet):
    tweet = re.sub(r'http\S+', '', tweet)  # Supprimer les URL
    tweet = re.sub(r'@\w+', '', tweet)     # Supprimer les mentions
    tweet = re.sub(r'#\w+', '', tweet)     # Supprimer les hashtags
    tweet = re.sub(r'[^\w\s]', '', tweet)  # Supprimer les caractères spéciaux
    return tweet

def tokenize_tweet(tweet):
    return word_tokenize(tweet)

def remove_stopwords(tokens):
    stop_words = set(stopwords.words('french'))
    return [token for token in tokens if token.lower() not in stop_words]

def analyze_sentiment(tweet):
    blob = TextBlob(tweet)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return "positif"
    elif sentiment < 0:
        return "négatif"
    else:
        return "neutre"

