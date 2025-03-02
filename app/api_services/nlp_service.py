from transformers import pipeline

sentiment_pipeline = pipeline("sentiment-analysis")

def analyze_text(text: str):
    """
    Perform sentiment analysis on given text.
    """
    return sentiment_pipeline(text)