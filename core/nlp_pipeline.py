import os
import pandas as pd
import nltk
from transformers import pipeline
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Download necessary NLP tools
nltk.download("punkt")
nltk.download("stopwords")

# Initialize NLP model
sentiment_pipeline = pipeline("sentiment-analysis")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["conflict_events"]

def run_nlp_processing():
    print("\n🧠 Running NLP Processing on Conflict Data...")

    cursor = collection.find({"nlp_processed": {"$exists": False}}).limit(100)
    documents = list(cursor)

    if not documents:
        print("✅ No new documents to process!")
        return

    for doc in documents:
        text = doc.get("actor1", "") + " " + doc.get("actor2", "")
        sentiment_result = sentiment_pipeline(text)[0]

        doc["nlp_analysis"] = {
            "sentiment": {
                "transformers": sentiment_result,
            },
        }
        doc["nlp_processed"] = True
        collection.update_one({"_id": doc["_id"]}, {"$set": doc})

    print(f"✅ Processed {len(documents)} documents!")

if __name__ == "__main__":
    run_nlp_processing()