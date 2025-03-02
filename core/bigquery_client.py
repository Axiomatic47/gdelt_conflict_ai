from google.cloud import bigquery
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# ✅ Initialize BigQuery & MongoDB Clients
bq_client = bigquery.Client()
mongo_client = MongoClient(os.getenv("MONGODB_URI"))
db = mongo_client["gdelt_db"]
collection = db["conflict_events"]

def fetch_and_store_gdelt():
    """
    Fetches conflict-related events from Google BigQuery and stores them in MongoDB.
    """
    print("\n📡 Fetching new GDELT conflict data from BigQuery...")

    query = """
    SELECT SQLDATE, Actor1Name, Actor2Name, SAFE_CAST(EventRootCode AS INT64) AS EventCode, 
           GoldsteinScale, AvgTone, ActionGeo_FullName
    FROM `gdelt-bq.gdeltv2.events`
    WHERE SAFE_CAST(EventRootCode AS INT64) BETWEEN 14 AND 19
      AND SQLDATE >= CAST(FORMAT_DATE("%Y%m%d", DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)) AS INT64)
    ORDER BY SQLDATE DESC
    LIMIT 100;
    """

    results = bq_client.query(query).result()

    documents = [
        {
            "date": str(row.SQLDATE),
            "actor1": row.Actor1Name,
            "actor2": row.Actor2Name,
            "event_code": row.EventCode,
            "goldstein_scale": row.GoldsteinScale,
            "avg_tone": row.AvgTone,
            "location": row.ActionGeo_FullName,
        }
        for row in results
    ]

    if documents:
        collection.insert_many(documents)
        print(f"✅ Successfully inserted {len(documents)} documents into MongoDB!")
    else:
        print("⚠️ No data fetched from BigQuery!")

if __name__ == "__main__":
    fetch_and_store_gdelt()