## events_routes.py - Combined Events Endpoint

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
import sys
import os
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()


# Generic Event model that works for both GDELT and ACLED
class Event(BaseModel):
    id: Optional[str] = None
    event_date: str
    event_type: str
    actor1: Optional[str] = None
    actor2: Optional[str] = None
    country: str
    location: Optional[str] = None
    latitude: float
    longitude: float
    data_source: str
    description: Optional[str] = None
    fatalities: Optional[int] = None
    avg_tone: Optional[float] = None
    goldstein_scale: Optional[float] = None
    event_code: Optional[str] = None
    intensity: Optional[float] = None


@router.get("/combined", response_model=List[Event])
async def get_combined_events(
        days: int = Query(30, description="Number of days of history to retrieve"),
        limit: int = Query(250, description="Maximum number of events to return per source")
):
    """
    Get combined events from both GDELT and ACLED sources
    """
    try:
        # Call internal API endpoints to get events from both sources
        # We use httpx for internal API calls
        import httpx

        logger.info(f"Fetching combined events for {days} days with limit {limit} per source")

        async with httpx.AsyncClient(base_url="http://localhost:4041") as client:
            # Fetch GDELT events
            gdelt_response = await client.get(f"/gdelt/events?days={days}&limit={limit}")
            gdelt_events = gdelt_response.json() if gdelt_response.status_code == 200 else []

            # Fetch ACLED events
            acled_response = await client.get(f"/acled/events?limit={limit}")
            acled_events = acled_response.json()["events"] if acled_response.status_code == 200 else []

        # Combine events
        combined_events = []
        combined_events.extend(gdelt_events)
        combined_events.extend(acled_events)

        # Sort by date (newest first)
        combined_events.sort(key=lambda x: x.get("event_date", ""), reverse=True)

        logger.info(f"Retrieved {len(combined_events)} combined events")
        return combined_events
    except Exception as e:
        logger.error(f"Error fetching combined events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


