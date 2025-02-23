import pandas as pd
import os
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

# Load the CSV file
csv_file = "nlp_conflict_analysis.csv"
if not os.path.exists(csv_file):
    print(f"❌ CSV file '{csv_file}' not found!")
    exit()

df = pd.read_csv(csv_file)

# Convert DataFrame to JSON format
data = df.to_dict(orient="records")

# Insert into MongoDB (Upsert: Avoid duplicate keys)
bulk_ops = [
    pymongo.UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True) for doc in data
]

if bulk_ops:
    collection.bulk_write(bulk_ops)

print(f"✅ Stored {len(data)} NLP results in MongoDB!")
