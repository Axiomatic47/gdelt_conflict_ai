import os
import time
import random
import requests
import json
from pymongo import MongoClient
from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# ✅ Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["gdelt_news"]

# ✅ Define GDELT API parameters
GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
QUERY_PARAMS = {
    "query": "conflict",  # Modify this query for specific searches
    "mode": "artlist",  # List of articles
    "maxrecords": 3,  # Reduce to avoid rate limits
    "format": "json"
}


# ✅ Function to fetch news from GDELT API with retry handling
def fetch_gdelt_news():
    attempt = 0
    max_attempts = 5  # Set max retries

    while attempt < max_attempts:
        try:
            print(f"\n🌎 Fetching GDELT news (Attempt {attempt + 1}/{max_attempts})...")
            response = requests.get(GDELT_API_URL, params=QUERY_PARAMS, timeout=10)

            # ✅ Success: Process articles
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])

                if not articles:
                    print("⚠️ No new articles found.")
                    return []

                print(f"✅ Retrieved {len(articles)} articles from GDELT.")
                return articles

            # 🚨 Handle API rate limits (429)
            elif response.status_code == 429:
                wait_time = 120 * (2 ** attempt) + random.randint(10, 30)  # Exponential backoff
                print(f"⚠️ 429 Error: Too Many Requests. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

            # 🚨 Other errors
            else:
                print(f"❌ Error fetching GDELT News: {response.status_code} - {response.text}")

        except requests.RequestException as e:
            print(f"❌ Request error: {e}")

        attempt += 1

    print("❌ Max retry attempts reached. Exiting.")
    return []


# ✅ Function to store news in MongoDB
def store_gdelt_news(articles):
    if not articles:
        print("⚠️ No new articles to store.")
        return

    new_count = 0
    updated_count = 0

    for article in articles:
        article_id = article.get("url", "")  # Using URL as unique identifier

        # ✅ Prepare MongoDB document
        news_data = {
            "_id": article_id,
            "title": article.get("title", "No Title"),
            "url": article.get("url"),
            "source": article.get("source", "Unknown"),
            "language": article.get("language", "Unknown"),
            "date": article.get("seendate", "Unknown"),
            "location": article.get("location", "Unknown"),
            "topics": article.get("themes", []),
            "social_shares": article.get("socialshares", {}),
            "summary": article.get("snippet", "No summary available"),
        }

        # ✅ Insert or update document
        result = collection.update_one({"_id": article_id}, {"$set": news_data}, upsert=True)

        if result.matched_count > 0:
            updated_count += 1
        else:
            new_count += 1

    print(f"\n✅ Stored GDELT News: {new_count} new articles, {updated_count} updated.")


# ✅ Run the full pipeline
if __name__ == "__main__":
    articles = fetch_gdelt_news()
    store_gdelt_news(articles)
