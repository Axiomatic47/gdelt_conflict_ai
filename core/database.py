import os
import json
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB connection string from .env file
MONGODB_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGODB_URI)
db = client["gdelt_conflict_db"]  # Database name
collection = db["conflict_events"]  # Collection name

def store_conflict_data(data):
    """
    Store conflict event data in MongoDB.
    """
    if isinstance(data, list):
        collection.insert_many(data)  # Insert multiple documents
    else:
        collection.insert_one(data)  # Insert single document
    print("✅ Data successfully stored in MongoDB.")

def fetch_latest_conflicts(limit=10):
    """
    Fetch latest stored conflict events from MongoDB.
    """
    results = collection.find().sort("_id", -1).limit(limit)
    return list(results)

if __name__ == "__main__":
    # Test MongoDB connection
    print("🔗 Connected to MongoDB:", db.name)
    print("📌 Existing collections:", db.list_collection_names())
