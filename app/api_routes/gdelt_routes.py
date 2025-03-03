from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import sys
import os

# Add appropriate core directories to path to import existing functions
sys.path.append(os.path.join(os.path.dirname(__file__), '../../core'))

router = APIRouter(
    prefix="/gdelt",
    tags=["gdelt-events"],
    responses={404: {"description": "Not found"}},
)

class EventData(BaseModel):
    date: str
    country: str
    event_count: int
    avg_tone: float
    event_codes: List[str]
    themes: List[str]

@router.get("/events", response_model=List[EventData])
async def get_gdelt_events():
    """
    Get GDELT event data for visualization
    """
    try:
        # This would call your function that retrieves GDELT event data
        # For now, we'll return placeholder data
        events = [
            {
                "date": "2025-03-01",
                "country": "United States",
                "event_count": 42,
                "avg_tone": -2.7,
                "event_codes": ["0211", "0231", "1122"],
                "themes": ["PROTEST", "GOVERNMENT", "SECURITY_SERVICES"]
            },
            {
                "date": "2025-03-01",
                "country": "China",
                "event_count": 37,
                "avg_tone": -3.5,
                "event_codes": ["1011", "1031", "1214"],
                "themes": ["MILITARY", "GOVERNMENT", "ECON"]
            }
            # Add more events as needed
        ]
        return events
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))