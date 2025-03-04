#!/usr/bin/env python3
"""
ACLED Client Module - Functions for fetching and processing ACLED conflict data
"""

import os
import json
import logging
import requests
import datetime
from typing import List, Dict, Any, Optional
import pymongo
from dotenv import load_dotenv

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# ACLED API access information
ACLED_API_URL = "https://api.acleddata.com/acled/read"
ACLED_API_KEY = os.getenv("ACLED_API_KEY", "")

# MongoDB connection string
MONGO_URI = os.getenv("MONGODB_URI", "")

# Local storage for development/fallback
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data")
ACLED_CACHE_FILE = os.path.join(DATA_DIR, "acled_events.json")

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Initialize MongoDB connection
try:
    mongo_client = pymongo.MongoClient(MONGO_URI)
    db = mongo_client["gdelt_db"]
    acled_collection = db["acled_events"]
    logger.info("✅ Connected to MongoDB successfully")
except Exception as e:
    logger.warning(f"MongoDB connection failed: {str(e)}")
    mongo_client = None
    acled_collection = None

def fetch_and_store_acled(days_back: int = 30, limit: int = 500) -> bool:
    """
    Fetch recent events from ACLED API and store them in MongoDB.

    Args:
        days_back: Number of days to look back for events
        limit: Maximum number of events to fetch

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Calculate date range
        end_date = datetime.datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime("%Y-%m-%d")

        logger.info(f"Fetching ACLED data from {start_date} to {end_date}")

        # Build API request parameters
        params = {
            "key": ACLED_API_KEY,
            "event_date": f"{start_date}|{end_date}",
            "event_date_where": "BETWEEN",
            "limit": limit,
            "terms": "accept",
        }

        # If no API key, use sample data
        if not ACLED_API_KEY:
            logger.warning("⚠️ No ACLED API key found, using sample data")
            return _store_sample_acled_data()

        # Make API request
        response = requests.get(ACLED_API_URL, params=params)

        # Check response
        if response.status_code != 200:
            logger.error(f"❌ ACLED API error: {response.status_code} - {response.text}")
            return _store_sample_acled_data()

        # Parse response
        data = response.json()
        events = data.get("data", [])
        logger.info(f"✅ Fetched {len(events)} events from ACLED API")

        # Save to local cache for development/fallback
        with open(ACLED_CACHE_FILE, "w") as f:
            json.dump(events, f, indent=2)
        logger.info(f"✅ Cached ACLED data to {ACLED_CACHE_FILE}")

        # Store in MongoDB if available
        if acled_collection:
            try:
                # Clear existing events in the date range
                acled_collection.delete_many({
                    "event_date": {"$gte": start_date, "$lte": end_date}
                })

                # Insert new events
                if events:
                    # Add timestamp to each event
                    for event in events:
                        event["fetched_at"] = datetime.datetime.now().isoformat()

                    acled_collection.insert_many(events)

                logger.info(f"✅ Stored {len(events)} ACLED events in MongoDB")
                return True
            except Exception as e:
                logger.error(f"❌ MongoDB storage error: {str(e)}")
                return False
        else:
            logger.warning("⚠️ MongoDB not available, data stored in local cache only")
            return True

    except Exception as e:
        logger.error(f"❌ Error fetching ACLED data: {str(e)}")
        return _store_sample_acled_data()

def _store_sample_acled_data() -> bool:
    """
    Create and store sample ACLED data for development and testing.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Sample data based on ACLED structure
        sample_events = [
            {
                "data_id": "1",
                "event_date": datetime.datetime.now().strftime("%Y-%m-%d"),
                "event_type": "Violence against civilians",
                "actor1": "Armed Group A",
                "actor2": "Civilians",
                "country": "Somalia",
                "location": "Mogadishu",
                "latitude": 2.0469,
                "longitude": 45.3182,
                "fatalities": 5,
                "notes": "Armed attack against civilian population",
                "source": "Local Media",
                "fetched_at": datetime.datetime.now().isoformat()
            },
            {
                "data_id": "2",
                "event_date": (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d"),
                "event_type": "Battle-No change of territory",
                "actor1": "Military Forces",
                "actor2": "Rebel Group B",
                "country": "Ethiopia",
                "location": "Addis Ababa",
                "latitude": 9.0084,
                "longitude": 38.7668,
                "fatalities": 12,
                "notes": "Clashes between military and rebel forces",
                "source": "International Observer",
                "fetched_at": datetime.datetime.now().isoformat()
            },
            {
                "data_id": "3",
                "event_date": (datetime.datetime.now() - datetime.timedelta(days=4)).strftime("%Y-%m-%d"),
                "event_type": "Riots",
                "actor1": "Protesters",
                "actor2": "Police Forces",
                "country": "Kenya",
                "location": "Nairobi",
                "latitude": -1.2921,
                "longitude": 36.8219,
                "fatalities": 2,
                "notes": "Protests over election results turned violent",
                "source": "Local Media",
                "fetched_at": datetime.datetime.now().isoformat()
            },
            {
                "data_id": "4",
                "event_date": (datetime.datetime.now() - datetime.timedelta(days=7)).strftime("%Y-%m-%d"),
                "event_type": "Violence against civilians",
                "actor1": "Militant Group C",
                "actor2": "Civilians",
                "country": "Nigeria",
                "location": "Lagos",
                "latitude": 6.5244,
                "longitude": 3.3792,
                "fatalities": 8,
                "notes": "Attack on village by militant group",
                "source": "NGO Report",
                "fetched_at": datetime.datetime.now().isoformat()
            }
        ]

        # Save to local cache
        with open(ACLED_CACHE_FILE, "w") as f:
            json.dump(sample_events, f, indent=2)
        logger.info(f"✅ Created sample ACLED data in {ACLED_CACHE_FILE}")

        # Store in MongoDB if available
        if acled_collection:
            try:
                # Clear existing sample data
                acled_collection.delete_many({"data_id": {"$in": ["1", "2", "3", "4"]}})

                # Insert sample data
                acled_collection.insert_many(sample_events)
                logger.info(f"✅ Stored sample ACLED data in MongoDB")
                return True
            except Exception as e:
                logger.error(f"❌ MongoDB storage error: {str(e)}")
                return False
        else:
            logger.warning("⚠️ MongoDB not available, sample data stored in local cache only")
            return True

    except Exception as e:
        logger.error(f"❌ Error creating sample ACLED data: {str(e)}")
        return False

def get_acled_data_for_heatmap() -> List[Dict[str, Any]]:
    """
    Retrieve ACLED event data for visualization in the heatmap.

    Returns:
        List[Dict]: List of ACLED events with coordinates and intensity
    """
    try:
        # Try to retrieve from MongoDB first
        if acled_collection:
            try:
                # Fetch the most recent events
                mongo_events = list(acled_collection.find({}).sort("fetched_at", -1).limit(100))

                if mongo_events:
                    logger.info(f"Retrieved {len(mongo_events)} ACLED events from MongoDB")

                    # Process MongoDB results for heatmap
                    return _process_events_for_heatmap(mongo_events)

            except Exception as e:
                logger.warning(f"MongoDB retrieval failed: {str(e)}")

        # Fall back to local file if MongoDB failed or returned no results
        if os.path.exists(ACLED_CACHE_FILE):
            with open(ACLED_CACHE_FILE, "r") as f:
                file_events = json.load(f)

            logger.info(f"Retrieved {len(file_events)} ACLED events from local file")

            # Process file results for heatmap
            return _process_events_for_heatmap(file_events)

        # If no data available, create and use sample data
        logger.warning("No ACLED data available, creating sample data")
        _store_sample_acled_data()

        with open(ACLED_CACHE_FILE, "r") as f:
            sample_events = json.load(f)

        logger.info(f"Using {len(sample_events)} sample ACLED events")

        # Process sample data for heatmap
        return _process_events_for_heatmap(sample_events)

    except Exception as e:
        logger.error(f"❌ Error getting ACLED data for heatmap: {str(e)}")
        return []

def _process_events_for_heatmap(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process ACLED events for heatmap visualization.

    Args:
        events: List of ACLED events from API or cache

    Returns:
        List[Dict]: Processed events with normalized data for heatmap
    """
    processed_events = []

    for event in events:
        # Ensure all required fields are present
        processed_event = {
            "id": event.get("data_id", str(hash(str(event)))),
            "event_date": event.get("event_date", ""),
            "event_type": event.get("event_type", "Unknown"),
            "country": event.get("country", "Unknown"),
            "location": event.get("location", ""),
            "latitude": float(event.get("latitude", 0)),
            "longitude": float(event.get("longitude", 0)),
            "data_source": "ACLED"
        }

        # Add optional fields if available
        if "actor1" in event:
            processed_event["actor1"] = event["actor1"]

        if "actor2" in event:
            processed_event["actor2"] = event["actor2"]

        if "fatalities" in event:
            processed_event["fatalities"] = int(event["fatalities"])

            # Calculate intensity based on fatalities (0-10 scale)
            # Higher fatalities = higher intensity
            # 0 fatalities = 3, 5 fatalities = 6, 10+ fatalities = 10
            processed_event["intensity"] = min(10, 3 + (int(event["fatalities"]) * 0.7))
        else:
            # Default intensity if no fatalities data
            processed_event["intensity"] = 5

        # Add description from notes if available
        if "notes" in event:
            processed_event["description"] = event["notes"]

        # Add source if available
        if "source" in event:
            processed_event["source"] = event["source"]

        processed_events.append(processed_event)

    return processed_events

# Run a test if executed directly
if __name__ == "__main__":
    print("🌐 ACLED Client Test")
    fetch_and_store_acled(days_back=30, limit=100)
    events = get_acled_data_for_heatmap()
    print(f"Retrieved {len(events)} events for heatmap")