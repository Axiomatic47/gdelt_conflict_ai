import os
import matplotlib

# ✅ Use non-interactive backend to avoid GUI issues on macOS or headless systems
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import seaborn as sns

# ✅ Prevent macOS Input Method Kit (IMK) errors
os.environ["QT_MAC_WANTS_LAYER"] = "1"

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

# Fetch NLP-processed data
print("\n🔍 Fetching NLP-processed conflict data from MongoDB...")
df = pd.DataFrame(list(collection.find({}, {"sentiment_transformers": 1, "_id": 0})))

# Ensure there's data to visualize
if df.empty:
    print("⚠️ No NLP-processed data found! Exiting visualization.")
    exit()

# Count sentiment occurrences
sentiment_counts = df["sentiment_transformers"].value_counts()

# ✅ Plot sentiment distribution
plt.figure(figsize=(8, 5))
sns.barplot(x=sentiment_counts.index, y=sentiment_counts.values, palette="coolwarm")
plt.xlabel("Sentiment Category")
plt.ylabel("Count")
plt.title("Distribution of Sentiment in Conflict Data")

# ✅ Save plot as an image instead of displaying in GUI
output_path = "visualizations/sentiment_distribution.png"
plt.savefig(output_path)
print(f"\n✅ Sentiment distribution saved to {output_path}")

# Close the plot to free memory
plt.close()
print("\n✅ NLP Visualization completed!\n")