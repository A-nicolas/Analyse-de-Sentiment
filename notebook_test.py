# Databricks notebook source
pip install pytest

# COMMAND ----------

# MAGIC %run ./notebook_fonctions

# COMMAND ----------



# Définissez les fonctions de test avec pytest
def test_clean_tweet():
    tweet = "Ce est un tweet de test avec des @mentions et des #hashtags !"
    cleaned_tweet = clean_tweet(tweet)
    assert cleaned_tweet == "Ce est un tweet de test avec des  et des  !"

def test_tokenize_tweet():
    tweet = "Ce est un tweet de test."
    tokens = tokenize_tweet(tweet)
    assert tokens == ["Ce", "est", "un", "tweet", "de", "test", "."]

def test_remove_stopwords():
    tokens = ["Ce", "est", "un", "tweet", "de", "test"]
    filtered_tokens = remove_stopwords(tokens)
    assert filtered_tokens == ["Ce", "tweet", "test"]

def test_analyze_sentiment():
    positive_tweet = "J'adore ce film, il est incroyable !"
    negative_tweet = "Ce film est horrible, je déteste !"
    neutral_tweet = "Ce film est assez moyen."
    
    positive_sentiment = analyze_sentiment(positive_tweet)
    negative_sentiment = analyze_sentiment(negative_tweet)
    neutral_sentiment = analyze_sentiment(neutral_tweet)
    
    assert positive_sentiment == "positif"
    assert negative_sentiment == "négatif"
    assert neutral_sentiment == "neutre"

