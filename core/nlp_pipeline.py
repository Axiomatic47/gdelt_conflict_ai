import os
import pymongo
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
from transformers import pipeline
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from textblob import TextBlob

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

# Ensure NLTK resources are downloaded
nltk.download("punkt")
nltk.download("stopwords")

# Initialize sentiment analysis pipeline
sentiment_pipeline = pipeline("sentiment-analysis")

# Fetch data from MongoDB
cursor = collection.find({"nlp_processed": {"$exists": False}})
documents = list(cursor)

if not documents:
    print("✅ No new documents to process!")
    exit()

# Process each document
for doc in documents:
    text = doc.get("location", None)  # Assuming 'location' is the primary text field

    if not text:
        print("⚠️ Warning: Skipping record with missing text.")
        continue

    # Transformers-based sentiment analysis
    transformers_result = sentiment_pipeline(text)[0]

    # TextBlob sentiment analysis
    blob = TextBlob(text)
    textblob_result = blob.sentiment.polarity

    # Store results in document
    doc["nlp_analysis"] = {
        "sentiment": {
            "transformers": transformers_result,
            "textblob": textblob_result,
        }
    }
    doc["nlp_processed"] = True  # Mark as processed

    print(f"\n🔍 Debug: {text[:50]}... → {transformers_result}")

# Bulk update processed documents
if documents:
    collection.bulk_write(
        [pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for doc in documents]
    )

print(f"✅ Successfully processed and stored {len(documents)} documents in MongoDB!")
