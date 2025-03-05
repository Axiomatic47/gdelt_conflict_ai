## gdelt_client.py - Enhanced GDELT Data Fetcher

"""
GDELT Client - Functions for fetching and processing GDELT data
"""
import logging
from typing import List, Dict, Any, Optional
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")
GDELT_API_KEY = os.getenv("GDELT_API_KEY", "")  # Optional API key

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to MongoDB if URI is provided
mongo_client = None
gdelt_collection = None

if MONGO_URI:
    try:
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["gdelt_db"]
        gdelt_collection = db["gdelt_events"]
        logger.info("Connected to MongoDB for GDELT data")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")


def fetch_gdelt_data(days_back: int = 30, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch conflict-related data from GDELT API or BigQuery

    Args:
        days_back: Number of days to look back
        limit: Maximum number of events to fetch

    Returns:
        List of GDELT event data
    """
    logger.info(f"Fetching GDELT data for past {days_back} days, limit {limit}")

    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)

    # Try to use BigQuery if available
    try:
        from google.cloud import bigquery

        # Initialize BigQuery client
        bq_client = bigquery.Client()

        # SQL query for GDELT events
        query = f"""
        SELECT 
            CAST(SQLDATE AS STRING) as date,
            Actor1Name as actor1,
            Actor2Name as actor2,
            CAST(EventRootCode AS STRING) as event_code,
            GoldsteinScale as goldstein_scale,
            AvgTone as avg_tone,
            ActionGeo_Lat as latitude,
            ActionGeo_Long as longitude,
            ActionGeo_CountryCode as country_code,
            ActionGeo_FullName as location
        FROM `gdelt-bq.gdeltv2.events`
        WHERE CAST(EventRootCode AS INT64) BETWEEN 14 AND 19  -- Conflict events
          AND SQLDATE >= {start_date.strftime('%Y%m%d')}
          AND ActionGeo_Lat IS NOT NULL
          AND ActionGeo_Long IS NOT NULL
        ORDER BY SQLDATE DESC
        LIMIT {limit}
        """

        results = bq_client.query(query).result()

        # Convert to list of dictionaries
        events = []
        for row in results:
            event = {
                "event_date": format_gdelt_date(row.date),
                "actor1": row.actor1,
                "actor2": row.actor2,
                "event_code": row.event_code,
                "event_type": get_event_type_name(row.event_code),
                "goldstein_scale": row.goldstein_scale,
                "avg_tone": row.avg_tone,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "country": get_country_name(row.country_code),
                "location": row.location,
                "data_source": "GDELT"
            }
            events.append(event)

        logger.info(f"Successfully fetched {len(events)} events from BigQuery")

        # Store in MongoDB if available
        if gdelt_collection:
            try:
                # Use bulk operations for efficiency
                if events:
                    gdelt_collection.insert_many(events)
                    logger.info(f"Stored {len(events)} GDELT events in MongoDB")
            except Exception as e:
                logger.error(f"Error storing GDELT events in MongoDB: {str(e)}")

        return events

    except ImportError:
        logger.warning("BigQuery not available, falling back to GDELT API")
    except Exception as e:
        logger.error(f"Error fetching from BigQuery: {str(e)}")

    # Fall back to GDELT API
    try:
        # GDELT API URL
        api_url = "https://api.gdeltproject.org/api/v2/doc/doc"

        # Query parameters
        params = {
            "query": "domain:conflict",
            "format": "json",
            "mode": "artlist",
            "maxrecords": limit,
            "timespan": f"{days_back}days"
        }

        # Add API key if available
        if GDELT_API_KEY:
            params["apikey"] = GDELT_API_KEY

        # Make API request
        response = requests.get(api_url, params=params)

        if response.status_code == 200:
            data = response.json()
            articles = data.get("articles", [])

            # Transform to our events format
            events = []
            for article in articles:
                # Extract location if available
                geo = article.get("geonames", [{}])[0] if article.get("geonames") else {}

                event = {
                    "event_date": article.get("seendate", end_date.strftime("%Y-%m-%d")),
                    "actor1": None,  # Not available from this API
                    "actor2": None,  # Not available from this API
                    "event_code": "14",  # Default to conflict
                    "event_type": "Conflict",  # Generic type
                    "goldstein_scale": None,  # Not available from this API
                    "avg_tone": article.get("tone", None),
                    "latitude": geo.get("lat", 0),
                    "longitude": geo.get("lon", 0),
                    "country": geo.get("countryname", "Unknown"),
                    "location": geo.get("name", article.get("location", None)),
                    "data_source": "GDELT",
                    "description": article.get("title", None)
                }
                events.append(event)

            logger.info(f"Successfully fetched {len(events)} events from GDELT API")

            # Store in MongoDB if available
            if gdelt_collection and events:
                try:
                    gdelt_collection.insert_many(events)
                    logger.info(f"Stored {len(events)} GDELT events in MongoDB")
                except Exception as e:
                    logger.error(f"Error storing GDELT events in MongoDB: {str(e)}")

            return events
        else:
            logger.error(f"GDELT API error: {response.status_code}, {response.text}")
            return []
    except Exception as e:
        logger.error(f"Error fetching from GDELT API: {str(e)}")
        return []


def fetch_gdelt_events(days_back: int = 30, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch GDELT events from MongoDB or fallback to API

    Args:
        days_back: Number of days to look back
        limit: Maximum number of events to fetch

    Returns:
        List of GDELT event data
    """
    logger.info(f"Fetching GDELT events for past {days_back} days, limit {limit}")

    # Try to fetch from MongoDB first
    if gdelt_collection:
        try:
            # Calculate date threshold
            date_threshold = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

            # Query MongoDB
            events = list(gdelt_collection.find(
                {"event_date": {"$gte": date_threshold}, "data_source": "GDELT"},
                {"_id": 0}  # Exclude MongoDB _id
            ).sort("event_date", -1).limit(limit))

            if events:
                logger.info(f"Retrieved {len(events)} GDELT events from MongoDB")
                return events
        except Exception as e:
            logger.error(f"Error fetching GDELT events from MongoDB: {str(e)}")

    # Fall back to fetching from GDELT
    logger.info("No events in MongoDB or MongoDB not available, fetching from GDELT")
    return fetch_gdelt_data(days_back=days_back, limit=limit)


def process_gdelt_data(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Process GDELT data to calculate SGM scores for countries

    Args:
        events: List of GDELT event data

    Returns:
        List of country data with SGM scores
    """
    logger.info(f"Processing {len(events)} GDELT events for SGM scoring")

    # Group events by country
    country_data = {}
    for event in events:
        country = event.get("country", "Unknown")
        country_code = event.get("country_code") or get_country_code(country)

        if country not in country_data:
            country_data[country] = {
                "code": country_code,
                "country": country,
                "events": [],
                "avg_tone_sum": 0,
                "goldstein_sum": 0,
                "latitude": event.get("latitude", 0),
                "longitude": event.get("longitude", 0)
            }

        country_data[country]["events"].append(event)
        country_data[country]["avg_tone_sum"] += event.get("avg_tone", 0) or 0
        country_data[country]["goldstein_sum"] += event.get("goldstein_scale", 0) or 0

    # Calculate SGM scores for each country
    sgm_scores = []
    for country, data in country_data.items():
        event_count = len(data["events"])
        if event_count == 0:
            continue

        # Calculate average tone and Goldstein scale
        avg_tone = data["avg_tone_sum"] / event_count
        avg_goldstein = data["goldstein_sum"] / event_count

        # More negative tone = higher international score (0-10 scale)
        intl_score = min(10, max(0, 5 - (avg_tone / 2)))

        # More negative Goldstein scale = higher domestic score (0-10 scale)
        # Goldstein scale is -10 to 10, so normalize to 0-10
        domestic_score = min(10, max(0, 5 - (avg_goldstein / 2)))

        # Calculate GSCS as average of domestic and international scores
        gscs = (domestic_score + intl_score) / 2

        # Calculate a simple stability score (STI)
        import random
        sti = int(gscs * 8) + random.randint(-10, 10)
        sti = max(0, min(100, sti))  # Ensure it's between 0-100

        # Determine category based on GSCS
        category = get_category(gscs)

        # Generate description
        description = generate_description(country, domestic_score, intl_score, gscs)

        sgm_scores.append({
            "code": data["code"],
            "country": country,
            "srsD": round(domestic_score, 1),
            "srsI": round(intl_score, 1),
            "gscs": round(gscs, 1),
            "sgm": round(gscs, 1),
            "sti": sti,
            "category": category,
            "description": description,
            "event_count": event_count,
            "avg_tone": round(avg_tone, 2),
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "updated_at": datetime.now().isoformat()
        })

    logger.info(f"Generated SGM scores for {len(sgm_scores)} countries")
    return sgm_scores


# Helper functions

def format_gdelt_date(date_str: str) -> str:
    """Format GDELT date string to ISO format"""
    if len(date_str) == 8:  # YYYYMMDD format
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    return date_str


def get_event_type_name(event_code: str) -> str:
    """Get human-readable event type from GDELT event code"""
    event_types = {
        "14": "Protest",
        "15": "Force Use",
        "16": "Reduce Relations",
        "17": "Coercion",
        "18": "Assault",
        "19": "Fight"
    }
    return event_types.get(event_code, "Conflict")


def get_country_code(country_name: str) -> str:
    """Get country code from country name"""
    # This is a very simplified mapping - in production use a proper country code library
    country_codes = {
        "United States": "US",
        "Russia": "RU",
        "China": "CN",
        "Germany": "DE",
        "France": "FR",
        "United Kingdom": "GB",
        "Japan": "JP",
        "India": "IN",
        "Brazil": "BR",
        "Canada": "CA",
        "Australia": "AU",
        "South Africa": "ZA"
    }

    # Try to match the country name
    for name, code in country_codes.items():
        if name.lower() in country_name.lower():
            return code

    # If no match found, return first two letters as a fallback
    return country_name[:2].upper()


def get_country_name(country_code: str) -> str:
    """Get country name from country code"""
    # This is a very simplified mapping - in production use a proper country code library
    country_names = {
        "US": "United States",
        "RU": "Russia",
        "CN": "China",
        "DE": "Germany",
        "FR": "France",
        "GB": "United Kingdom",
        "JP": "Japan",
        "IN": "India",
        "BR": "Brazil",
        "CA": "Canada",
        "AU": "Australia",
        "ZA": "South Africa"
    }
    return country_names.get(country_code, country_code)


def get_category(gscs: float) -> str:
    """Determine the supremacism category based on GSCS score"""
    if gscs <= 2:
        return "Non-Supremacist Governance"
    elif gscs <= 4:
        return "Mixed Governance"
    elif gscs <= 6:
        return "Soft Supremacism"
    elif gscs <= 8:
        return "Structural Supremacism"
    else:
        return "Extreme Supremacism"


def generate_description(country: str, domestic: float, international: float, gscs: float) -> str:
    """Generate a simple description based on the scores"""
    if gscs <= 2:
        return f"{country} demonstrates low levels of supremacism with generally egalitarian governance patterns."
    elif gscs <= 4:
        return f"{country} shows mixed governance with some egalitarian and some supremacist tendencies."
    elif gscs <= 6:
        return f"{country} exhibits soft supremacism with institutional inequalities despite formal legal equality."
    elif gscs <= 8:
        return f"{country} demonstrates structural supremacism with notable inequalities at societal and governmental levels."
    else:
        return f"{country} shows extreme supremacist governance with severe systemic discrimination."
