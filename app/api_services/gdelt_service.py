from google.cloud import bigquery
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from bson import ObjectId  # ✅ Fix for MongoDB ObjectId serialization

# Load environment variables
load_dotenv()

# Initialize BigQuery Client
bq_client = bigquery.Client()

# Initialize MongoDB Client
mongo_client = MongoClient(os.getenv("MONGODB_URI"))
db = mongo_client["gdelt_db"]
news_collection = db["gdelt_news"]

def fetch_gdelt_news():
    """
    Fetch recent news articles related to conflict from GDELT BigQuery.
    Stores the articles in MongoDB and returns them.
    """
    print("\n📡 Fetching latest GDELT news from BigQuery...")

    query = """
    SELECT 
        DocumentIdentifier AS url,
        V2Themes AS themes,
        SAFE_CAST(PARSE_DATE('%Y%m%d', LEFT(CAST(Date AS STRING), 8)) AS DATE) AS event_date,  -- ✅ Fix timestamp parsing
        SourceCommonName AS source, 
        V2Locations AS locations, 
        V2Tone AS tone
    FROM `gdelt-bq.gdeltv2.gkg`
    WHERE LOWER(V2Themes) LIKE '%conflict%'
      AND SAFE_CAST(PARSE_DATE('%Y%m%d', LEFT(CAST(Date AS STRING), 8)) AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- ✅ Ensure valid date range
    ORDER BY event_date DESC
    LIMIT 50;
    """

    try:
        query_job = bq_client.query(query)
        results = query_job.result()

        articles = []
        for row in results:
            article = {
                "url": row.url,
                "date": str(row.event_date),  # ✅ Readable format YYYY-MM-DD
                "source": row.source,
                "themes": row.themes,
                "locations": row.locations,
                "tone": row.tone
            }
            articles.append(article)

        # Store in MongoDB, ensuring no duplicates
        if articles:
            for article in articles:
                news_collection.update_one(
                    {"url": article["url"]}, {"$set": article}, upsert=True
                )
            print(f"✅ Stored {len(articles)} articles in MongoDB!")

        # ✅ Convert `_id` to a string before returning JSON
        for article in articles:
            article["_id"] = str(ObjectId())

        return articles
    except Exception as e:
        print(f"❌ Error fetching news from BigQuery: {e}")
        return {"error": f"Failed to fetch news from BigQuery: {e}"}

# Ensure the function runs if called directly
if __name__ == "__main__":
    fetch_gdelt_news()