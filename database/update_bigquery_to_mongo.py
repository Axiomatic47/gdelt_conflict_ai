from google.cloud import bigquery
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv()

# MongoDB Setup
MONGO_URI = os.getenv("MONGODB_URI")
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["conflict_events"]

# Google BigQuery Setup
bigquery_client = bigquery.Client()

# Logging setup
log_filename = "bigquery_to_mongo.log"
logging.basicConfig(filename=log_filename, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Query GDELT for latest conflict events
query = """
SELECT SQLDATE, Actor1Name, Actor2Name, EventCode, GoldsteinScale, AvgTone, ActionGeo_FullName
FROM `gdelt-bq.gdeltv2.events`
WHERE EventRootCode BETWEEN 14 AND 19  -- Conflict-related event codes
  AND DATE(TIMESTAMP_SECONDS(SQLDATE)) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
ORDER BY SQLDATE DESC
LIMIT 100;
"""

try:
    logging.info("Starting BigQuery fetch...")
    query_job = bigquery_client.query(query)
    results = query_job.result()

    # Transform results into a list of dictionaries
    data = []
    for row in results:
        data.append({
            "date": row.SQLDATE,
            "actor1": row.Actor1Name,
            "actor2": row.Actor2Name,
            "event_code": row.EventCode,
            "goldstein_scale": row.GoldsteinScale,
            "avg_tone": row.AvgTone,
            "location": row.ActionGeo_FullName
        })

    if data:
        collection.insert_many(data)
        logging.info(f"✅ Successfully inserted {len(data)} records into MongoDB.")

    else:
        logging.info("⚠️ No new conflict events found.")

except Exception as e:
    logging.error(f"❌ Failed to fetch and store BigQuery data: {str(e)}")

print("✅ Process completed! Check logs for details.")

