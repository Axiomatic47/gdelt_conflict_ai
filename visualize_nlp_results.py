import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

# Fetch NLP-processed conflict data
df = pd.DataFrame(list(collection.find({}, {"sentiment_transformers": 1, "_id": 0})))

# Check if data exists
if df.empty:
    print("❌ No NLP data found! Exiting...")
    exit()

# Plot Sentiment Distribution
plt.figure(figsize=(8, 6))
sns.countplot(x="sentiment_transformers", data=df, order=df["sentiment_transformers"].value_counts().index)
plt.title("Distribution of Sentiment in Conflict Data")
plt.xlabel("Sentiment")
plt.ylabel("Count")
plt.xticks(rotation=45)
plt.show()
