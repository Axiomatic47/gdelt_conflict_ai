import os
import requests
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GDELT_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


def fetch_gdelt_news(query: str, max_records: int = 10):
    """Fetches real-time news from GDELT DOC 2.0 API."""
    params = {
        "query": query,
        "mode": "artlist",  # List of articles
        "maxrecords": max_records,
        "format": "json"
    }

    response = requests.get(GDELT_API_URL, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch GDELT data: {response.status_code}")
