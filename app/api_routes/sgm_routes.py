# Python Backend API Improvements

## sgm_routes.py - Improving Data Retrieval

from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uuid
import sys
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your existing SGM data service
try:
    from core.sgm_data_service import get_country_sgm_data, get_country_detail, run_sgm_analysis
except ImportError:
    logger.warning("Could not import SGM core functions. Using placeholders.")


    # Placeholder functions if imports fail
    def get_country_sgm_data():
        return []


    def get_country_detail(country_code):
        return None


    def run_sgm_analysis():
        return True

# Create router
router = APIRouter()


# Pydantic models for request/response validation
class CountryData(BaseModel):
    code: str
    country: str
    sgm: float
    gscs: Optional[float] = None
    srsD: Optional[float] = None
    srsI: Optional[float] = None
    latitude: float
    longitude: float
    sti: Optional[float] = None
    category: Optional[str] = None
    description: Optional[str] = None
    event_count: Optional[int] = None
    avg_tone: Optional[float] = None
    updated_at: Optional[str] = None


class RegionalData(BaseModel):
    region: str
    avg_sgm: float
    countries: int
    highest_country: str
    highest_sgm: float
    lowest_country: str
    lowest_sgm: float


class AnalysisResponse(BaseModel):
    jobId: str
    status: str


class AnalysisStatusResponse(BaseModel):
    jobId: str
    status: str
    progress: Optional[float] = None
    message: Optional[str] = None


# Dictionary to store job status (replace with database in production)
analysis_jobs = {}


# Background task for running analysis
def run_analysis_task(job_id: str):
    try:
        # Call your existing analysis function
        logger.info(f"Starting analysis job {job_id}")
        run_sgm_analysis()
        logger.info(f"Completed analysis job {job_id}")

        # Update job status to completed
        analysis_jobs[job_id] = {
            "status": "completed",
            "progress": 1.0,
            "message": "Analysis completed successfully"
        }
    except Exception as e:
        logger.error(f"Error in analysis job {job_id}: {str(e)}")

        # Update job status to failed
        analysis_jobs[job_id] = {
            "status": "failed",
            "progress": 0,
            "message": f"Analysis failed: {str(e)}"
        }


@router.get("/countries", response_model=List[CountryData])
async def get_all_countries(
        limit: int = Query(200, description="Maximum number of countries to return"),
        include_details: bool = Query(True, description="Include detailed descriptions")
):
    """
    Get SGM data for all countries with pagination and filtering options
    """
    try:
        # Call your existing function to get country data with improved parameters
        countries = get_country_sgm_data(limit=limit, include_details=include_details)
        logger.info(f"Retrieved SGM data for {len(countries)} countries")
        return countries
    except Exception as e:
        logger.error(f"Error fetching country data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/countries/{country_code}", response_model=CountryData)
async def get_country(country_code: str):
    """
    Get detailed data for a specific country
    """
    try:
        # Call your existing function to get country detail
        country = get_country_detail(country_code)
        if not country:
            logger.warning(f"Country {country_code} not found")
            raise HTTPException(status_code=404, detail=f"Country {country_code} not found")
        logger.info(f"Retrieved SGM data for country {country_code}")
        return country
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching country {country_code}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/run-analysis", response_model=AnalysisResponse)
async def trigger_analysis(background_tasks: BackgroundTasks):
    """
    Trigger a new SGM analysis run with background processing
    """
    try:
        job_id = str(uuid.uuid4())

        # Store initial job status
        analysis_jobs[job_id] = {
            "status": "started",
            "progress": 0.0,
            "message": "Analysis started"
        }

        # Add analysis task to background tasks
        background_tasks.add_task(run_analysis_task, job_id)

        logger.info(f"Started analysis job {job_id}")
        return {"jobId": job_id, "status": "started"}
    except Exception as e:
        logger.error(f"Error starting analysis: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/analysis-status/{job_id}", response_model=AnalysisStatusResponse)
async def get_analysis_status(job_id: str):
    """
    Get the status of an analysis job
    """
    try:
        if job_id not in analysis_jobs:
            raise HTTPException(status_code=404, detail=f"Analysis job {job_id} not found")

        job_status = analysis_jobs[job_id]
        logger.info(f"Retrieved status for analysis job {job_id}: {job_status['status']}")

        return {
            "jobId": job_id,
            "status": job_status["status"],
            "progress": job_status.get("progress"),
            "message": job_status.get("message")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting analysis status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


```

## gdelt_routes.py - Adding Better GDELT Event Retrieval

```python
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

# Import your existing GDELT functions
try:
    from core.gdelt_service import fetch_gdelt_events
except ImportError:
    logger.warning("Could not import GDELT core functions. Using placeholders.")


    # Placeholder functions if imports fail
    def fetch_gdelt_events(days_back=30, limit=100):
        return []

# Create router
router = APIRouter()


# Pydantic models for request/response validation
class EventData(BaseModel):
    id: Optional[str] = None
    event_date: str
    event_type: str
    actor1: Optional[str] = None
    actor2: Optional[str] = None
    country: str
    location: Optional[str] = None
    latitude: float
    longitude: float
    data_source: str = "GDELT"
    description: Optional[str] = None
    avg_tone: Optional[float] = None
    goldstein_scale: Optional[float] = None
    event_code: Optional[str] = None
    intensity: Optional[float] = None


@router.get("/events", response_model=List[EventData])
async def get_gdelt_events(
        days: int = Query(30, description="Number of days of history to retrieve"),
        limit: int = Query(500, description="Maximum number of events to return")
):
    """
    Get GDELT event data for visualization with improved filtering options
    """
    try:
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        logger.info(f"Fetching GDELT events from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

        # Call your existing function to get GDELT events
        events = fetch_gdelt_events(days_back=days, limit=limit)

        # Transform data to standard format if needed
        transformed_events = []
        for event in events:
            # Ensure each event has required fields
            event_data = {
                "id": event.get("id", f"gdelt-{len(transformed_events)}"),
                "event_date": event.get("event_date", datetime.now().strftime("%Y-%m-%d")),
                "event_type": event.get("event_type", "Unknown"),
                "actor1": event.get("actor1"),
                "actor2": event.get("actor2"),
                "country": event.get("country", "Unknown"),
                "location": event.get("location"),
                "latitude": float(event.get("latitude", 0)),
                "longitude": float(event.get("longitude", 0)),
                "data_source": "GDELT",
                "description": event.get("description"),
                "avg_tone": event.get("avg_tone"),
                "goldstein_scale": event.get("goldstein_scale"),
                "event_code": event.get("event_code"),
                # Calculate intensity if not provided
                "intensity": event.get("intensity") or (
                    5 - float(event.get("goldstein_scale", 0)) / 2
                    if event.get("goldstein_scale") is not None else 5
                )
            }
            transformed_events.append(event_data)

        logger.info(f"Retrieved {len(transformed_events)} GDELT events")
        return transformed_events
    except Exception as e:
        logger.error(f"Error fetching GDELT events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


```

## acled_routes.py - Adding ACLED Event Support

```python
from fastapi import APIRouter, HTTPException, BackgroundTasks, Query
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uuid
import sys
import os
import logging
from datetime import datetime, timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your existing ACLED functions
try:
    from core.acled_client import fetch_acled_data, get_stored_events
except ImportError:
    logger.warning("Could not import ACLED core functions. Using placeholders.")


    # Placeholder functions if imports fail
    def fetch_acled_data(days_back=30, limit=100):
        return True


    def get_stored_events(limit=100):
        return []

# Create router
router = APIRouter()


# Pydantic models for request/response validation
class AcledEvent(BaseModel):
    id: Optional[str] = None
    event_date: str
    event_type: str
    actor1: Optional[str] = None
    actor2: Optional[str] = None
    country: str
    location: Optional[str] = None
    latitude: float
    longitude: float
    data_source: str = "ACLED"
    description: Optional[str] = None
    fatalities: Optional[int] = None
    intensity: Optional[float] = None


class EventsResponse(BaseModel):
    events: List[AcledEvent]
    count: int
    source: str = "ACLED"


class FetchResponse(BaseModel):
    jobId: str
    status: str
    message: Optional[str] = None


class FetchStatusResponse(BaseModel):
    jobId: str
    status: str
    progress: Optional[float] = None
    message: Optional[str] = None


# Dictionary to store job status (replace with database in production)
fetch_jobs = {}


# Background task for running ACLED fetch
def run_acled_fetch_task(job_id: str, days_back: int, limit: int):
    try:
        # Update job status to in progress
        fetch_jobs[job_id] = {
            "status": "in_progress",
            "progress": 0.1,
            "message": "Starting ACLED data fetch"
        }

        logger.info(f"Starting ACLED fetch job {job_id} for {days_back} days with limit {limit}")

        # Call your existing function to fetch ACLED data
        success = fetch_acled_data(days_back=days_back, limit=limit)

        # Update job status based on result
        if success:
            fetch_jobs[job_id] = {
                "status": "completed",
                "progress": 1.0,
                "message": f"Successfully fetched ACLED data for {days_back} days"
            }
            logger.info(f"Completed ACLED fetch job {job_id}")
        else:
            fetch_jobs[job_id] = {
                "status": "failed",
                "progress": 0,
                "message": "Failed to fetch ACLED data"
            }
            logger.error(f"Failed ACLED fetch job {job_id}")
    except Exception as e:
        logger.error(f"Error in ACLED fetch job {job_id}: {str(e)}")

        # Update job status to failed
        fetch_jobs[job_id] = {
            "status": "failed",
            "progress": 0,
            "message": f"ACLED fetch failed: {str(e)}"
        }


@router.get("/events", response_model=EventsResponse)
async def get_acled_events(
        limit: int = Query(500, description="Maximum number of events to return")
):
    """
    Get stored ACLED event data
    """
    try:
        # Call your existing function to get stored ACLED events
        events = get_stored_events(limit=limit)

        # Transform data if needed
        transformed_events = []
        for event in events:
            # Ensure each event has required fields
            event_data = {
                "id": event.get("id", f"acled-{len(transformed_events)}"),
                "event_date": event.get("event_date", datetime.now().strftime("%Y-%m-%d")),
                "event_type": event.get("event_type", "Unknown"),
                "actor1": event.get("actor1"),
                "actor2": event.get("actor2"),
                "country": event.get("country", "Unknown"),
                "location": event.get("location"),
                "latitude": float(event.get("latitude", 0)),
                "longitude": float(event.get("longitude", 0)),
                "data_source": "ACLED",
                "description": event.get("description"),
                "fatalities": event.get("fatalities"),
                # Calculate intensity based on event type and fatalities if not provided
                "intensity": event.get("intensity") or calculate_intensity(event)
            }
            transformed_events.append(event_data)

        logger.info(f"Retrieved {len(transformed_events)} ACLED events")
        return {"events": transformed_events, "count": len(transformed_events)}
    except Exception as e:
        logger.error(f"Error fetching ACLED events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/fetch", response_model=FetchResponse)
async def trigger_acled_fetch(
        background_tasks: BackgroundTasks,
        days_back: int = Query(30, description="Number of days of history to fetch"),
        limit: int = Query(500, description="Maximum number of events to fetch")
):
    """
    Trigger a new ACLED data fetch with configurable parameters
    """
    try:
        job_id = str(uuid.uuid4())

        # Store initial job status
        fetch_jobs[job_id] = {
            "status": "started",
            "progress": 0.0,
            "message": "ACLED fetch started"
        }

        # Add fetch task to background tasks
        background_tasks.add_task(run_acled_fetch_task, job_id, days_back, limit)

        logger.info(f"Started ACLED fetch job {job_id} for {days_back} days with limit {limit}")
        return {"jobId": job_id, "status": "started", "message": f"Fetching ACLED data for {days_back} days"}
    except Exception as e:
        logger.error(f"Error starting ACLED fetch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status/{job_id}", response_model=FetchStatusResponse)
async def get_acled_status(job_id: str):
    """
    Get the status of an ACLED fetch job
    """
    try:
        if job_id not in fetch_jobs:
            raise HTTPException(status_code=404, detail=f"ACLED fetch job {job_id} not found")

        job_status = fetch_jobs[job_id]
        logger.info(f"Retrieved status for ACLED fetch job {job_id}: {job_status['status']}")

        return {
            "jobId": job_id,
            "status": job_status["status"],
            "progress": job_status.get("progress"),
            "message": job_status.get("message")
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting ACLED fetch status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Helper function to calculate intensity for ACLED events
def calculate_intensity(event):
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
    fatalities = event.get("fatalities", 0) or 0
    fatality_adjustment = 0
    if fatalities > 0:
        fatality_adjustment = min(3, fatalities // 5)  # Cap at +3

    # Calculate final intensity (0-10 scale)
    intensity = base_intensity + type_adjustment + fatality_adjustment
    intensity = max(0, min(10, intensity))  # Ensure within 0-10 range

    return intensity


```

## events_routes.py - Combined Events Endpoint

```python
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


```

## sgm_data_service.py - Enhanced Country Data Fetching

```python
"""
SGM Data Service - Core functions for Supremacism Global Metric analysis
"""
import logging
import datetime
import random
from typing import List, Dict, Any, Optional
from pymongo import MongoClient
import os
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
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["gdelt_db"]
        sgm_collection = db["sgm_scores"]
        logger.info("Connected to MongoDB for SGM data")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")


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
        "updated_at": datetime.datetime.now().isoformat()
    },
    # Add more sample countries as needed...
]
```

## gdelt_client.py - Enhanced GDELT Data Fetcher

```python
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


```

## acled_client.py - ACLED Data Fetcher

```python
"""
ACLED Client - Functions for fetching and processing ACLED data
"""
import logging
from typing import List, Dict, Any, Optional, Union, Tuple
import requests
from datetime import datetime, timedelta
import os
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
        mongo_client = MongoClient(MONGO_URI)
        db = mongo_client["gdelt_db"]
        acled_collection = db["acled_events"]
        logger.info("Connected to MongoDB for ACLED data")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        logger.warning("MongoDB connection failed: %s", str(e))


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
```

## main.py - Updated FastAPI Server Setup

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import API routes
from app.api_routes import sgm_routes, gdelt_routes, acled_routes, events_routes

# Initialize FastAPI app
app = FastAPI(
    title="Supremacism Analysis API",
    description="API for analyzing GDELT and ACLED data to calculate Supremacism-Egalitarianism metrics",
    version="1.0.0",
)

# Enable CORS for frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict to your frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API endpoints
app.include_router(sgm_routes.router, prefix="/sgm", tags=["SGM"])
app.include_router(gdelt_routes.router, prefix="/gdelt", tags=["GDELT"])
app.include_router(acled_routes.router, prefix="/acled", tags=["ACLED"])
app.include_router(events_routes.router, prefix="/events", tags=["Events"])


# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Welcome to the Supremacism Analysis API",
        "endpoints": {
            "SGM": "/sgm - Supremacism Global Metrics",
            "GDELT": "/gdelt - GDELT conflict events",
            "ACLED": "/acled - ACLED conflict events",
            "Events": "/events - Combined conflict events"
        }
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=4041, reload=True)