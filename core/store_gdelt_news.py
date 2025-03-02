import requests
import time
import random
import json
from pymongo import MongoClient
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Initialize MongoDB
client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["gdelt_news"]

# GDELT API URL
GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Headers to Mimic a Real User
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

# Query Parameters
QUERY_PARAMS = {
    "query": "conflict",
    "mode": "artlist",
    "maxrecords": 3,
    "format": "json"
}

MAX_RETRIES = 5  # Maximum retry attempts
INITIAL_SLEEP = 30  # Initial wait time in seconds


def fetch_gdelt_news():
    """
    Fetch news articles from GDELT API with retry and exponential backoff.
    """
    retry_count = 0
    while retry_count < MAX_RETRIES:
        try:
            print(f"\n🌎 Fetching GDELT news (Attempt {retry_count + 1}/{MAX_RETRIES})...")
            response = requests.get(GDELT_API_URL, headers=HEADERS, params=QUERY_PARAMS, timeout=10)

            # If request is successful, process data
            if response.status_code == 200:
                data = response.json()
                articles = data.get("articles", [])
                if not articles:
                    print("⚠️ No new articles found.")
                    return

                print(f"✅ {len(articles)} articles fetched successfully!")
                store_articles(articles)
                return  # Exit loop if successful

            # Handle 429 Too Many Requests Error
            elif response.status_code == 429:
                wait_time = INITIAL_SLEEP * (2 ** retry_count)  # Exponential backoff
                print(f"⚠️ 429 Error: Too Many Requests. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)

            else:
                print(f"❌ Unexpected Error: {response.status_code} - {response.text}")
                break  # Exit loop on non-retryable errors

        except requests.RequestException as e:
            print(f"❌ Request Failed: {str(e)}")

        retry_count += 1

    print("🚨 Max retries reached. Exiting.")


def store_articles(articles):
    """
    Store articles in MongoDB, avoiding duplicates.
    """
    existing_urls = {doc["url"] for doc in collection.find({}, {"url": 1})}

    new_articles = []
    for article in articles:
        if article["url"] not in existing_urls:
            new_articles.append({
                "url": article["url"],
                "title": article["title"],
                "seendate": article["seendate"],
                "source": article.get("domain", "Unknown"),
                "language": article.get("language", "Unknown"),
                "sourcecountry": article.get("sourcecountry", "Unknown"),
                "image": article.get("socialimage", ""),
            })

    if new_articles:
        collection.insert_many(new_articles)
        print(f"✅ Stored {len(new_articles)} new articles in MongoDB!")
    else:
        print("⚠️ No new articles to store.")


if __name__ == "__main__":
    fetch_gdelt_news()
