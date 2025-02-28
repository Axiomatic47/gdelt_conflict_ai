from google.cloud import bigquery
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize BigQuery Client
bq_client = bigquery.Client()

# Initialize MongoDB Client
mongo_client = MongoClient(os.getenv("MONGODB_URI"))
db = mongo_client["gdelt_db"]  # Create or connect to "gdelt_db"
collection = db["conflict_events"]  # Create or connect to "conflict_events"

def fetch_and_store_gdelt():
    """
    Fetches conflict-related events from Google BigQuery and stores them in MongoDB.
    """
    print("\n📡 Fetching new GDELT conflict data from BigQuery...")

    # ✅ FIX: Use SQLDATE (YYYYMMDD INT) instead of `_PARTITIONTIME`
    query = """
    SELECT SQLDATE, Actor1Name, Actor2Name, SAFE_CAST(EventRootCode AS INT64) AS EventCode, 
           GoldsteinScale, AvgTone, ActionGeo_FullName
    FROM `gdelt-bq.gdeltv2.events`
    WHERE SAFE_CAST(EventRootCode AS INT64) BETWEEN 14 AND 19  -- Conflict-related event codes
      AND SQLDATE >= CAST(FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS INT64)  -- Last 30 days
    ORDER BY SQLDATE DESC
    LIMIT 100;
    """

    # Run Query
    query_job = bq_client.query(query)
    results = query_job.result()

    # Convert Results to MongoDB Documents
    documents = []
    for row in results:
        document = {
            "date": str(row.SQLDATE),
            "actor1": row.Actor1Name,
            "actor2": row.Actor2Name,
            "event_code": row.EventCode,
            "goldstein_scale": row.GoldsteinScale,
            "avg_tone": row.AvgTone,
            "location": row.ActionGeo_FullName,
        }
        documents.append(document)

    # Store Data in MongoDB
    if documents:
        collection.insert_many(documents)
        print(f"✅ Successfully inserted {len(documents)} documents into MongoDB!")
    else:
        print("⚠️ No data fetched from BigQuery!")

# Ensure function is available for import
if __name__ == "__main__":
    fetch_and_store_gdelt()
