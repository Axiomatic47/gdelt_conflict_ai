"""
ACLED Client - Functions for fetching and processing ACLED data
"""
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
import requests
from datetime import datetime, timedelta
import os
import certifi
import ssl
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")
ACLED_API_KEY = os.getenv("ACLED_API_KEY", "")  # API key for ACLED

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to MongoDB if URI is provided
mongo_client = None
acled_collection = None

if MONGO_URI:
    try:
        # Use certifi for proper certificate validation
        mongo_client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=10000
        )

        # Force a connection test
        mongo_client.admin.command('ping')

        db = mongo_client["gdelt_db"]
        acled_collection = db["acled_events"]
        logger.info("✅ Connected to MongoDB for ACLED data successfully!")
    except Exception as e:
        logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
        logger.warning("MongoDB connection failed for ACLED: %s", str(e))


def fetch_acled_data(days_back: int = 30, limit: int = 500) -> bool:
    """
    Fetch conflict event data from ACLED API

    Args:
        days_back: Number of days to look back
        limit: Maximum number of events to fetch

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info(f"Fetching ACLED data for past {days_back} days, limit {limit}")

    # Skip if no API key available
    if not ACLED_API_KEY:
        logger.error("ACLED API key not found. Set ACLED_API_KEY environment variable.")
        return False

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    # Format dates for ACLED API (YYYY-MM-DD)
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # ACLED API URL
    api_url = "https://api.acleddata.com/acled/read"

    # Query parameters
    params = {
        "key": ACLED_API_KEY,
        "email": os.getenv("ACLED_EMAIL", "your.email@example.com"),
        "limit": limit,
        "event_date": f"{start_date_str}|{end_date_str}",
        "event_date_where": "BETWEEN",
        "format": "json"
    }

    try:
        # Make API request
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            events = data.get("data", [])

            # Transform to our events format
            transformed_events = []
            for event in events:
                transformed_event = {
                    "id": event.get("data_id", f"acled-{len(transformed_events)}"),
                    "event_date": event.get("event_date", end_date.strftime("%Y-%m-%d")),
                    "event_type": event.get("event_type", "Unknown"),
                    "actor1": event.get("actor1", None),
                    "actor2": event.get("actor2", None),
                    "country": event.get("country", "Unknown"),
                    "location": event.get("location", None),
                    "latitude": float(event.get("latitude", 0)),
                    "longitude": float(event.get("longitude", 0)),
                    "data_source": "ACLED",
                    "description": event.get("notes", None),
                    "fatalities": int(event.get("fatalities", 0)),
                    "intensity": calculate_intensity(event)
                }
                transformed_events.append(transformed_event)

            logger.info(f"Successfully fetched {len(transformed_events)} events from ACLED API")

            # Store in MongoDB if available
            if acled_collection and transformed_events:
                try:
                    # Use bulk operations for efficiency
                    bulk_ops = []
                    for event in transformed_events:
                        bulk_ops.append({
                            'updateOne': {
                                'filter': {'id': event['id']},
                                'update': {'$set': event},
                                'upsert': True
                            }
                        })

                    if bulk_ops:
                        acled_collection.bulk_write(bulk_ops)
                        logger.info(f"Stored {len(transformed_events)} ACLED events in MongoDB")
                except Exception as e:
                    logger.error(f"Error storing ACLED events in MongoDB: {str(e)}")

            return True
        else:
            logger.error(f"ACLED API error: {response.status_code}, {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error fetching from ACLED API: {str(e)}")
        return False


def get_stored_events(limit: int = 500) -> List[Dict[str, Any]]:
    """
    Get stored ACLED events from MongoDB

    Args:
        limit: Maximum number of events to retrieve

    Returns:
        List of ACLED event data
    """
    logger.info(f"Retrieving up to {limit} ACLED events from storage")

    # Try to fetch from MongoDB
    if acled_collection:
        try:
            events = list(acled_collection.find(
                {"data_source": "ACLED"},
                {"_id": 0}  # Exclude MongoDB _id
            ).sort("event_date", -1).limit(limit))

            logger.info(f"Retrieved {len(events)} ACLED events from MongoDB")
            return events
        except Exception as e:
            logger.error(f"Error fetching ACLED events from MongoDB: {str(e)}")

    # Fall back to sample data
    logger.warning("MongoDB not available or empty, using sample ACLED data")
    return SAMPLE_ACLED_EVENTS


def calculate_intensity(event: Dict[str, Any]) -> float:
    """Calculate intensity score (0-10) for an ACLED event"""
    base_intensity = 5  # Default mid-range intensity

    # Adjust based on event type
    event_type_map = {
        "Violence against civilians": 2,  # Increase intensity
        "Battle": 3,  # Significant increase
        "Explosion/Remote violence": 3,  # Significant increase
        "Riots": 1,  # Slight increase
        "Protests": 0,  # No change
        "Strategic development": -1  # Decrease intensity
    }

    event_type = event.get("event_type", "")
    type_adjustment = event_type_map.get(event_type, 0)

    # Adjust based on fatalities
    fatalities = int(event.get("fatalities", 0)) or 0
    fatality_adjustment = 0
    if fatalities > 0:
        fatality_adjustment = min(3, fatalities // 5)  # Cap at +3

    # Calculate final intensity (0-10 scale)
    intensity = base_intensity + type_adjustment + fatality_adjustment
    intensity = max(0, min(10, intensity))  # Ensure within 0-10 range

    return intensity


# Sample ACLED events for when API/MongoDB is not available
SAMPLE_ACLED_EVENTS = [
    {
        "id": "acled-1",
        "event_date": datetime.now().strftime("%Y-%m-%d"),
        "event_type": "Violence against civilians",
        "actor1": "Military Forces",
        "actor2": "Civilians",
        "country": "Somalia",
        "location": "Mogadishu",
        "latitude": 2.0469,
        "longitude": 45.3182,
        "data_source": "ACLED",
        "description": "Military forces attacked civilians in Mogadishu",
        "fatalities": 3,
        "intensity": 7
    },
    {
        "id": "acled-2",
        "event_date": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
        "event_type": "Battle",
        "actor1": "Rebel Group",
        "actor2": "Government Forces",
        "country": "Sudan",
        "location": "Khartoum",
        "latitude": 15.5007,
        "longitude": 32.5599,
        "data_source": "ACLED",
        "description": "Rebel forces engaged in battle with government troops",
        "fatalities": 12,
        "intensity": 9
    },
    {
        "id": "acled-3",
        "event_date": (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d"),
        "event_type": "Riots",
        "actor1": "Protesters",
        "actor2": "Police Forces",
        "country": "Nigeria",
        "location": "Lagos",
        "latitude": 6.5244,
        "longitude": 3.3792,
        "data_source": "ACLED",
        "description": "Riots broke out in Lagos after fuel price increases",
        "fatalities": 0,
        "intensity": 6
    }
]