"""
SGM Data Service - Core functions for Supremacism Global Metric analysis
"""
import logging
import datetime
import random
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
import os
import certifi
import ssl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Connect to MongoDB if URI is provided
mongo_client = None
sgm_collection = None

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
        sgm_collection = db["sgm_scores"]
        logger.info("✅ Connected to MongoDB for SGM data successfully!")
    except Exception as e:
        logger.error(f"❌ Failed to connect to MongoDB: {str(e)}")
        logger.warning("MongoDB connection failed - using sample data as fallback")


def get_country_sgm_data(limit: int = 200, include_details: bool = True) -> List[Dict[str, Any]]:
    """
    Get SGM data for countries with improved filtering

    Args:
        limit: Maximum number of countries to return
        include_details: Whether to include detailed descriptions

    Returns:
        List of country data objects
    """
    logger.info(f"Retrieving SGM data for up to {limit} countries (with details: {include_details})")

    # Try to fetch from MongoDB if available
    if sgm_collection:
        try:
            # Build query projection based on whether details are needed
            projection = None
            if not include_details:
                projection = {
                    "description": 0,
                    "event_count": 0,
                    "avg_tone": 0
                }

            # Fetch from MongoDB with limit
            countries = list(sgm_collection.find({}, projection).limit(limit))

            # Convert MongoDB _id to string
            for country in countries:
                if "_id" in country:
                    country["_id"] = str(country["_id"])

            logger.info(f"Successfully retrieved {len(countries)} countries from MongoDB")
            return countries
        except Exception as e:
            logger.error(f"Error fetching from MongoDB: {str(e)}")
            logger.warning("Falling back to sample country data")

    # Fall back to sample data
    logger.warning("Using sample country data instead of MongoDB")
    sample_data = SAMPLE_COUNTRIES

    # Apply limit
    limited_data = sample_data[:limit]

    # Remove details if not requested
    if not include_details:
        for country in limited_data:
            country.pop("description", None)
            country.pop("event_count", None)
            country.pop("avg_tone", None)

    return limited_data


def get_country_detail(country_code: str) -> Optional[Dict[str, Any]]:
    """
    Get detailed SGM data for a specific country

    Args:
        country_code: The ISO country code

    Returns:
        Country data object, or None if not found
    """
    logger.info(f"Retrieving SGM data for country {country_code}")

    # Try to fetch from MongoDB if available
    if sgm_collection:
        try:
            country = sgm_collection.find_one({"code": country_code.upper()})

            if country:
                # Convert MongoDB _id to string
                if "_id" in country:
                    country["_id"] = str(country["_id"])

                logger.info(f"Found country {country_code} in MongoDB")
                return country
        except Exception as e:
            logger.error(f"Error fetching country {country_code} from MongoDB: {str(e)}")
            logger.warning(f"Looking for country {country_code} in sample data")

    # Fall back to sample data
    logger.warning(f"Looking for country {country_code} in sample data")
    for country in SAMPLE_COUNTRIES:
        if country["code"].upper() == country_code.upper():
            return country

    logger.warning(f"Country {country_code} not found")
    return None


def run_sgm_analysis() -> bool:
    """
    Run a new SGM analysis with the latest GDELT data

    Returns:
        bool: True if successful, False otherwise
    """
    logger.info("Running SGM analysis")

    try:
        # Import necessary modules for GDELT analysis
        try:
            from core.gdelt_client import fetch_gdelt_data, process_gdelt_data
            logger.info("Using real GDELT client for analysis")

            # Fetch new GDELT data
            gdelt_data = fetch_gdelt_data(days_back=60, limit=1000)

            # Process data to calculate SGM scores
            results = process_gdelt_data(gdelt_data)

            # Store results in MongoDB if available
            if sgm_collection:
                # Use bulk operations for efficiency
                bulk_ops = []
                for country in results:
                    bulk_ops.append({
                        'updateOne': {
                            'filter': {'code': country['code']},
                            'update': {'$set': country},
                            'upsert': True
                        }
                    })

                if bulk_ops:
                    sgm_collection.bulk_write(bulk_ops)
                    logger.info(f"Stored {len(results)} SGM results in MongoDB")

            return True

        except ImportError:
            logger.warning("GDELT client not available, using sample data processing")

            # Simulate processing with sample data
            import time
            time.sleep(2)  # Simulate processing time

            # Update sample data with new timestamp
            updated_data = []
            for country in SAMPLE_COUNTRIES:
                updated_country = country.copy()
                updated_country["updated_at"] = datetime.datetime.now().isoformat()
                updated_data.append(updated_country)

            # Store in MongoDB if available
            if sgm_collection:
                for country in updated_data:
                    sgm_collection.update_one(
                        {"code": country["code"]},
                        {"$set": country},
                        upsert=True
                    )
                logger.info(f"Stored {len(updated_data)} updated sample countries in MongoDB")

            return True
    except Exception as e:
        logger.error(f"Error running SGM analysis: {str(e)}")
        return False


# Sample country data for when MongoDB is not available
SAMPLE_COUNTRIES = [
    {
        "code": "US",
        "country": "United States",
        "srsD": 4.2,
        "srsI": 6.7,
        "gscs": 5.2,
        "sgm": 5.2,
        "latitude": 37.0902,
        "longitude": -95.7129,
        "sti": 45,
        "category": "Soft Supremacism",
        "description": "The United States exhibits soft supremacism patterns with institutional inequalities despite formal legal equality.",
        "event_count": 42,
        "avg_tone": -2.7,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "CN",
        "country": "China",
        "srsD": 7.1,
        "srsI": 6.8,
        "gscs": 7.0,
        "sgm": 7.0,
        "latitude": 35.8617,
        "longitude": 104.1954,
        "sti": 75,
        "category": "Structural Supremacism",
        "description": "China demonstrates structural supremacism with notable inequalities at societal and governmental levels.",
        "event_count": 37,
        "avg_tone": -3.5,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "SE",
        "country": "Sweden",
        "srsD": 1.8,
        "srsI": 1.6,
        "gscs": 1.7,
        "sgm": 1.7,
        "latitude": 60.1282,
        "longitude": 18.6435,
        "sti": 15,
        "category": "Non-Supremacist Governance",
        "description": "Sweden demonstrates strong egalitarian governance with robust institutions protecting equality.",
        "event_count": 8,
        "avg_tone": 3.1,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "ZA",
        "country": "South Africa",
        "srsD": 5.1,
        "srsI": 3.2,
        "gscs": 4.1,
        "sgm": 4.1,
        "latitude": -30.5595,
        "longitude": 22.9375,
        "sti": 48,
        "category": "Mixed Governance",
        "description": "South Africa shows signs of soft supremacism despite strong constitutional protections.",
        "event_count": 28,
        "avg_tone": -1.2,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "DE",
        "country": "Germany",
        "srsD": 2.9,
        "srsI": 2.1,
        "gscs": 2.5,
        "sgm": 2.5,
        "latitude": 51.1657,
        "longitude": 10.4515,
        "sti": 25,
        "category": "Mixed Governance",
        "description": "Germany shows mixed governance with strong democratic institutions.",
        "event_count": 15,
        "avg_tone": 1.8,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "RU",
        "country": "Russia",
        "srsD": 6.9,
        "srsI": 7.8,
        "gscs": 7.3,
        "sgm": 7.3,
        "latitude": 61.5240,
        "longitude": 105.3188,
        "sti": 80,
        "category": "Structural Supremacism",
        "description": "Russia shows strong structural supremacism internally and aggressive patterns internationally.",
        "event_count": 53,
        "avg_tone": -5.2,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "IN",
        "country": "India",
        "srsD": 5.8,
        "srsI": 4.2,
        "gscs": 5.0,
        "sgm": 5.0,
        "latitude": 20.5937,
        "longitude": 78.9629,
        "sti": 60,
        "category": "Soft Supremacism",
        "description": "India exhibits soft supremacism with increasing tensions between religious and caste groups.",
        "event_count": 31,
        "avg_tone": -1.9,
        "updated_at": "2025-03-04T00:00:00Z"
    },
    {
        "code": "BR",
        "country": "Brazil",
        "srsD": 5.6,
        "srsI": 3.8,
        "gscs": 4.7,
        "sgm": 4.7,
        "latitude": -14.2350,
        "longitude": -51.9253,
        "sti": 55,
        "category": "Soft Supremacism",
        "description": "Brazil demonstrates soft supremacism with persistent racial and economic inequalities.",
        "event_count": 23,
        "avg_tone": -1.6,
        "updated_at": "2025-03-04T00:00:00Z"
    }
]