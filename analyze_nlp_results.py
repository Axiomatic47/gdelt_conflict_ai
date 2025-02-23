import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["nlp_analysis_results"]

# Fetch NLP-processed data
print("🔍 Fetching NLP-processed conflict data from MongoDB...")
data = list(collection.find({}, {"_id": 0, "sentiment": 1, "entities": 1}))

if not data:
    print("❌ No NLP-processed data found. Run NLP analysis first.")
    exit()

# Convert to DataFrame
df = pd.DataFrame(data)

# 📊 Interactive Sentiment Distribution Chart
sentiment_counts = df["sentiment"].value_counts().reset_index()
sentiment_counts.columns = ["Sentiment", "Count"]

fig_sentiment = px.bar(
    sentiment_counts,
    x="Sentiment",
    y="Count",
    color="Sentiment",
    title="Interactive Sentiment Distribution in Conflict Data",
    labels={"Count": "Number of Events", "Sentiment": "Sentiment Type"},
    template="plotly_dark"
)
fig_sentiment.show()

# 🔍 Extract Named Entities
entities_list = []
for doc in df["entities"]:
    if isinstance(doc, list):
        entities_list.extend(doc)

# Create Entity Frequency DataFrame
entity_df = pd.DataFrame(entities_list, columns=["Entity"])
entity_counts = entity_df["Entity"].value_counts().reset_index()
entity_counts.columns = ["Entity", "Count"]

# 📌 Limit to top 15 most mentioned entities
entity_counts = entity_counts.head(15)

# 📊 Interactive Named Entity Chart
fig_entities = px.bar(
    entity_counts,
    x="Entity",
    y="Count",
    color="Entity",
    title="Top 15 Named Entities in Conflict Data",
    labels={"Count": "Number of Mentions", "Entity": "Named Entity"},
    template="plotly_dark"
)
fig_entities.show()

print("✅ Interactive visualizations displayed!")
