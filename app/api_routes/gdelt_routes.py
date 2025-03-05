from fastapi import APIRouter, HTTPException
import logging
from typing import List, Dict, Any
import sys
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Try to import your core functions
try:
    from core.gdelt_client import fetch_gdelt_events
except ImportError:
    logger.warning("Could not import GDELT core functions. Using placeholders.")


    # Placeholder function
    def fetch_gdelt_events():
        return []

# Create router
router = APIRouter()


@router.get("/events")
async def get_gdelt_events():
    """
    Get GDELT event data for visualization
    """
    logger.info("Fetching GDELT events")

    # Sample GDELT events for when API access fails
    sample_events = [
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

    try:
        # Try to fetch real data first
        events = fetch_gdelt_events()
        if not events:
            logger.info("No GDELT events found, using sample data")
            events = sample_events
    except Exception as e:
        logger.error(f"Error fetching GDELT events: {e}")
        events = sample_events

    return events