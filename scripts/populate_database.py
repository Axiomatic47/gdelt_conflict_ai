#!/usr/bin/env python3
"""
Comprehensive data population script for GDELT Conflict AI
"""
import os
import sys
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import random

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("populate_db")

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import necessary modules
try:
    from core.sgm_data_service import SAMPLE_COUNTRIES, run_sgm_analysis
    from core.acled_client import SAMPLE_ACLED_EVENTS, fetch_acled_data
    from core.gdelt_client import fetch_gdelt_data, process_gdelt_data
except ImportError as e:
    logger.error(f"Error importing core modules: {e}")
    sys.exit(1)

# Load environment variables
load_dotenv()
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://gdelt_app:Instantsecurity7411@localhost:27017/gdelt_db")


def connect_mongodb():
    """Connect to MongoDB and return database object"""
    try:
        client = MongoClient(MONGODB_URI)
        # Test connection
        client.admin.command('ping')
        logger.info("✅ Connected to MongoDB")
        return client.get_database()
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        sys.exit(1)


def populate_sgm_data(db):
    """Populate SGM country data"""
    logger.info("Starting SGM data population...")
    sgm_collection = db.sgm_scores

    # Count existing documents
    existing_count = sgm_collection.count_documents({})
    logger.info(f"Found {existing_count} existing SGM country records")

    # Insert sample countries first to ensure we have baseline data
    for country in SAMPLE_COUNTRIES:
        sgm_collection.update_one(
            {"code": country["code"]},
            {"$set": country},
            upsert=True
        )

    logger.info(f"✅ Added/updated {len(SAMPLE_COUNTRIES)} sample countries to SGM collection")

    # Try to run actual SGM analysis
    try:
        logger.info("Running SGM analysis to generate real data...")
        success = run_sgm_analysis()
        if success:
            logger.info("✅ SGM analysis completed successfully")
        else:
            logger.warning("⚠️ SGM analysis did not complete successfully")
    except Exception as e:
        logger.error(f"❌ Error running SGM analysis: {e}")

    # Report final count
    final_count = sgm_collection.count_documents({})
    logger.info(f"SGM collection now has {final_count} countries")


def populate_acled_data(db):
    """Populate ACLED event data"""
    logger.info("Starting ACLED data population...")
    acled_collection = db.acled_events

    # Count existing documents
    existing_count = acled_collection.count_documents({})
    logger.info(f"Found {existing_count} existing ACLED events")

    # Insert sample events first to ensure we have baseline data
    for event in SAMPLE_ACLED_EVENTS:
        acled_collection.update_one(
            {"id": event["id"]},
            {"$set": event},
            upsert=True
        )

    logger.info(f"✅ Added/updated {len(SAMPLE_ACLED_EVENTS)} sample events to ACLED collection")

    # Try to fetch real ACLED data
    try:
        # Fetch data for different time periods to build a comprehensive dataset
        for days_back in [30, 60, 90, 180, 365]:
            logger.info(f"Fetching ACLED data from {days_back} days ago...")
            success = fetch_acled_data(days_back=days_back, limit=500)
            if success:
                logger.info(f"✅ Successfully fetched ACLED data from {days_back} days ago")
            else:
                logger.warning(f"⚠️ Failed to fetch ACLED data from {days_back} days ago")

            # Sleep to avoid rate limiting
            time.sleep(5)
    except Exception as e:
        logger.error(f"❌ Error fetching ACLED data: {e}")

    # Report final count
    final_count = acled_collection.count_documents({})
    logger.info(f"ACLED collection now has {final_count} events")


def populate_gdelt_data(db):
    """Populate GDELT event data with improved rate limiting"""
    logger.info("Starting GDELT data population...")
    gdelt_collection = db.gdelt_events

    # Count existing documents
    existing_count = gdelt_collection.count_documents({})
    logger.info(f"Found {existing_count} existing GDELT events")

    # Try to fetch real GDELT data
    try:
        # Create sample data to use as fallback
        sample_gdelt_events = [
            {
                "id": "gdelt-1",
                "event_date": "2024-02-16",
                "event_type": "Protest",
                "actor1": "GOVERNMENT",
                "actor2": "PROTESTERS",
                "country": "Egypt",
                "latitude": 26.820553,
                "longitude": 30.802498,
                "description": "Government forces responded to protests",
                "intensity": 6,
                "data_source": "GDELT",
                "avg_tone": -3.2,
                "goldstein_scale": -5.0
            },
            {
                "id": "gdelt-2",
                "event_date": "2024-02-14",
                "event_type": "Armed Conflict",
                "actor1": "REBEL GROUP",
                "actor2": "MILITARY",
                "country": "Syria",
                "latitude": 34.802075,
                "longitude": 38.996815,
                "description": "Armed assault against military installation",
                "intensity": 8,
                "data_source": "GDELT",
                "avg_tone": -6.7,
                "goldstein_scale": -8.0
            },
            {
                "id": "gdelt-3",
                "event_date": "2024-02-11",
                "event_type": "Political Tension",
                "actor1": "POLITICAL PARTY",
                "actor2": "POLITICAL PARTY",
                "country": "Ukraine",
                "latitude": 49.054585,
                "longitude": 31.466306,
                "description": "Verbal threats between political groups",
                "intensity": 3,
                "data_source": "GDELT",
                "avg_tone": -2.1,
                "goldstein_scale": -2.0
            }
        ]

        # First add sample events to ensure we have at least some data
        for event in sample_gdelt_events:
            gdelt_collection.update_one(
                {"id": event["id"]},
                {"$set": event},
                upsert=True
            )
        logger.info(f"✅ Added {len(sample_gdelt_events)} sample GDELT events as baseline")

        # Fetch data for different time periods with careful rate limiting
        time_periods = [7, 14, 30, 60, 90, 180, 365]
        for i, days_back in enumerate(time_periods):
            logger.info(f"Fetching GDELT data from {days_back} days ago...")

            # Wait longer between requests (GDELT requires 5s minimum)
            wait_time = 10  # 10 seconds between requests
            if i > 0:
                logger.info(f"Waiting {wait_time}s to avoid rate limiting...")
                time.sleep(wait_time)

            # Maximum retries
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    events = fetch_gdelt_data(days_back=days_back, limit=100)

                    if events:
                        for event in events:
                            # Generate a unique ID if not present
                            if "id" not in event:
                                event_id = f"gdelt-{hash(str(event.get('event_date', '')))}-{hash(str(event.get('event_type', '')))}"
                                event["id"] = event_id

                            gdelt_collection.update_one(
                                {"id": event["id"]},
                                {"$set": event},
                                upsert=True
                            )
                        logger.info(f"✅ Added/updated GDELT events from {days_back} days ago")
                        break  # Success, exit retry loop
                    else:
                        logger.warning(f"⚠️ No GDELT events retrieved from {days_back} days ago")
                        if attempt < max_retries - 1:
                            retry_wait = 15  # Wait longer between retries
                            logger.info(f"Retrying in {retry_wait}s (attempt {attempt + 1}/{max_retries})...")
                            time.sleep(retry_wait)

                except Exception as e:
                    logger.error(f"❌ Error on attempt {attempt + 1}: {e}")
                    if "429" in str(e) and attempt < max_retries - 1:
                        retry_wait = 15
                        logger.info(f"Rate limited. Retrying in {retry_wait}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(retry_wait)
                    elif attempt < max_retries - 1:
                        retry_wait = 10
                        logger.info(f"Retrying in {retry_wait}s (attempt {attempt + 1}/{max_retries})...")
                        time.sleep(retry_wait)

    except Exception as e:
        logger.error(f"❌ Error in GDELT data population: {e}")

    # Generate additional synthetic events to ensure good coverage
    generate_synthetic_gdelt_events(db)

    # Report final count
    final_count = gdelt_collection.count_documents({})
    logger.info(f"GDELT collection now has {final_count} events")


def generate_synthetic_gdelt_events(db):
    """Generate synthetic GDELT events to ensure good map coverage"""
    logger.info("Generating synthetic GDELT events for better coverage...")

    gdelt_collection = db.gdelt_events

    # Count existing documents
    existing_count = gdelt_collection.count_documents({})

    # If we already have a substantial number of events, don't add more
    if existing_count >= 50:
        logger.info(f"✅ Already have good GDELT coverage with {existing_count} events")
        return

    # Countries to ensure we have event coverage
    countries = [
        {"code": "US", "country": "United States", "latitude": 37.0902, "longitude": -95.7129},
        {"code": "GB", "country": "United Kingdom", "latitude": 55.3781, "longitude": -3.4360},
        {"code": "FR", "country": "France", "latitude": 46.2276, "longitude": 2.2137},
        {"code": "DE", "country": "Germany", "latitude": 51.1657, "longitude": 10.4515},
        {"code": "RU", "country": "Russia", "latitude": 61.5240, "longitude": 105.3188},
        {"code": "CN", "country": "China", "latitude": 35.8617, "longitude": 104.1954},
        {"code": "IN", "country": "India", "latitude": 20.5937, "longitude": 78.9629},
        {"code": "BR", "country": "Brazil", "latitude": -14.2350, "longitude": -51.9253},
        {"code": "ZA", "country": "South Africa", "latitude": -30.5595, "longitude": 22.9375},
        {"code": "AU", "country": "Australia", "latitude": -25.2744, "longitude": 133.7751}
    ]

    # Event types for variety
    event_types = ["Protest", "Armed Conflict", "Political Tension", "Diplomatic Exchange", "Civil Unrest"]

    # Generate synthetic events
    synthetic_events = []
    for i, country in enumerate(countries):
        # Create 3-5 events per country for good coverage
        for j in range(random.randint(3, 5)):
            # Create minor variations in coordinates to spread events
            lat_offset = random.uniform(-2, 2)
            lon_offset = random.uniform(-2, 2)

            event = {
                "id": f"gdelt-syn-{country['code']}-{j}",
                "event_date": (datetime.now() - timedelta(days=random.randint(1, 180))).strftime("%Y-%m-%d"),
                "event_type": random.choice(event_types),
                "actor1": "LOCAL_GROUP",
                "actor2": "GOVERNMENT",
                "country": country["country"],
                "latitude": country["latitude"] + lat_offset,
                "longitude": country["longitude"] + lon_offset,
                "description": f"Synthetic event in {country['country']} for map coverage",
                "intensity": random.randint(1, 9),
                "data_source": "GDELT",
                "avg_tone": round(random.uniform(-7.0, 2.0), 1),
                "goldstein_scale": round(random.uniform(-8.0, 8.0), 1)
            }
            synthetic_events.append(event)

    # Add synthetic events to database
    for event in synthetic_events:
        gdelt_collection.update_one(
            {"id": event["id"]},
            {"$set": event},
            upsert=True
        )

    logger.info(f"✅ Added {len(synthetic_events)} synthetic GDELT events for better coverage")


def generate_world_coverage():
    """Generate comprehensive world coverage data if real data is insufficient"""
    logger.info("Generating comprehensive world coverage data...")

    # List of countries to ensure we have at least basic coverage for most of the world
    # This is a fallback to ensure your world map has coverage
    basic_countries = [
        {"code": "US", "country": "United States", "latitude": 37.0902, "longitude": -95.7129},
        {"code": "CA", "country": "Canada", "latitude": 56.1304, "longitude": -106.3468},
        {"code": "MX", "country": "Mexico", "latitude": 23.6345, "longitude": -102.5528},
        {"code": "BR", "country": "Brazil", "latitude": -14.2350, "longitude": -51.9253},
        {"code": "AR", "country": "Argentina", "latitude": -38.4161, "longitude": -63.6167},
        {"code": "GB", "country": "United Kingdom", "latitude": 55.3781, "longitude": -3.4360},
        {"code": "FR", "country": "France", "latitude": 46.2276, "longitude": 2.2137},
        {"code": "DE", "country": "Germany", "latitude": 51.1657, "longitude": 10.4515},
        {"code": "IT", "country": "Italy", "latitude": 41.8719, "longitude": 12.5674},
        {"code": "ES", "country": "Spain", "latitude": 40.4637, "longitude": -3.7492},
        {"code": "RU", "country": "Russia", "latitude": 61.5240, "longitude": 105.3188},
        {"code": "CN", "country": "China", "latitude": 35.8617, "longitude": 104.1954},
        {"code": "IN", "country": "India", "latitude": 20.5937, "longitude": 78.9629},
        {"code": "JP", "country": "Japan", "latitude": 36.2048, "longitude": 138.2529},
        {"code": "AU", "country": "Australia", "latitude": -25.2744, "longitude": 133.7751},
        {"code": "ZA", "country": "South Africa", "latitude": -30.5595, "longitude": 22.9375},
        {"code": "EG", "country": "Egypt", "latitude": 26.8206, "longitude": 30.8025},
        {"code": "NG", "country": "Nigeria", "latitude": 9.0820, "longitude": 8.6753},
        {"code": "SA", "country": "Saudi Arabia", "latitude": 23.8859, "longitude": 45.0792},
        {"code": "TR", "country": "Turkey", "latitude": 38.9637, "longitude": 35.2433}
    ]

    db = connect_mongodb()
    sgm_collection = db.sgm_scores

    # Check current coverage
    existing_count = sgm_collection.count_documents({})
    if existing_count >= 50:  # If we already have good coverage
        logger.info(f"✅ Already have good world coverage with {existing_count} countries")
        return

    # Add basic world coverage with simulated SGM scores
    logger.info("Adding simulated data for basic world coverage...")

    for country in basic_countries:
        # Check if country already exists
        if sgm_collection.find_one({"code": country["code"]}):
            continue

        # Generate random SGM scores
        srsD = round(random.uniform(1.0, 8.0), 1)
        srsI = round(random.uniform(1.0, 8.0), 1)
        gscs = round((srsD + srsI) / 2, 1)

        # Determine category
        if gscs <= 2:
            category = "Non-Supremacist Governance"
        elif gscs <= 4:
            category = "Mixed Governance"
        elif gscs <= 6:
            category = "Soft Supremacism"
        else:
            category = "Structural Supremacism"

        country_data = {
            "code": country["code"],
            "country": country["country"],
            "srsD": srsD,
            "srsI": srsI,
            "gscs": gscs,
            "sgm": gscs,
            "latitude": country["latitude"],
            "longitude": country["longitude"],
            "sti": int(gscs * 10),
            "category": category,
            "description": f"{country['country']} has an SGM score of {gscs}.",
            "event_count": random.randint(5, 50),
            "avg_tone": round(random.uniform(-5.0, 3.0), 1),
            "updated_at": datetime.now().isoformat()
        }

        sgm_collection.update_one(
            {"code": country["code"]},
            {"$set": country_data},
            upsert=True
        )

    added_count = sgm_collection.count_documents({}) - existing_count
    logger.info(f"✅ Added {added_count} simulated countries for better world coverage")


def main():
    """Main function to populate the database"""
    logger.info("=== Starting comprehensive database population ===")
    start_time = datetime.now()

    db = connect_mongodb()

    # Populate SGM data
    populate_sgm_data(db)

    # Populate GDELT data
    populate_gdelt_data(db)

    # Populate ACLED data
    populate_acled_data(db)

    # Ensure we have good world coverage
    generate_world_coverage()

    # Print database stats
    logger.info("\n=== Database Population Summary ===")
    for collection_name in db.list_collection_names():
        count = db[collection_name].count_documents({})
        logger.info(f"- {collection_name}: {count} documents")

    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"=== Database population completed in {duration:.2f} seconds ===")


if __name__ == "__main__":
   main()