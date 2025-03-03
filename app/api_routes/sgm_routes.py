from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import uuid
import sys
import os
import logging
import json
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import your existing SGM data service
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
try:
    from core.sgm_data_service import get_country_sgm_data, get_country_detail, run_sgm_analysis
except ImportError:
    logger.warning("Could not import SGM core functions - using stub implementations")


    # Placeholder/stub functions if imports fail
    def get_country_sgm_data():
        # Read from sample_countries.json if it exists
        try:
            with open(os.path.join(os.path.dirname(__file__), "../../data/sample_countries.json"), "r") as f:
                return json.load(f)
        except:
            # Return empty list if file not found
            return []


    def get_country_detail(country_code):
        # Read from sample_countries.json if it exists, then find the matching country
        try:
            with open(os.path.join(os.path.dirname(__file__), "../../data/sample_countries.json"), "r") as f:
                countries = json.load(f)
                for country in countries:
                    if country.get("code") == country_code:
                        return country
        except:
            pass
        return None


    def run_sgm_analysis():
        return True

# Create router
router = APIRouter()

# Store for analysis jobs (replace with database in production)
analysis_jobs = {}


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


# Background task for running analysis
def run_analysis_task(job_id: str):
    try:
        # Set initial job state
        analysis_jobs[job_id] = {
            "status": "running",
            "progress": 0.1,
            "message": "Analysis started"
        }

        logger.info(f"Starting analysis job {job_id}")

        # Simulate progress updates
        for progress in [0.25, 0.5, 0.75]:
            # In a real implementation, this would be async or in a worker
            import time
            time.sleep(2)  # Simulate work
            analysis_jobs[job_id]["progress"] = progress
            analysis_jobs[job_id]["message"] = f"Processing data ({int(progress * 100)}%)"

        # Call actual analysis function
        success = run_sgm_analysis()

        # Update job status
        if success:
            analysis_jobs[job_id] = {
                "status": "completed",
                "progress": 1.0,
                "message": "Analysis completed successfully"
            }
        else:
            analysis_jobs[job_id] = {
                "status": "failed",
                "progress": 1.0,
                "message": "Analysis failed"
            }

        logger.info(f"Completed analysis job {job_id}")
    except Exception as e:
        logger.error(f"Error in analysis job {job_id}: {str(e)}")
        analysis_jobs[job_id] = {
            "status": "failed",
            "progress": 0,
            "message": f"Error: {str(e)}"
        }


@router.get("/countries", response_model=List[CountryData])
async def get_all_countries():
    """
    Get SGM data for all countries
    """
    try:
        # Call your existing function to get country data
        countries = get_country_sgm_data()
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
    Trigger a new SGM analysis run
    """
    try:
        job_id = str(uuid.uuid4())
        background_tasks.add_task(run_analysis_task, job_id)

        # Store job status
        analysis_jobs[job_id] = {
            "status": "started",
            "progress": 0.0,
            "message": "Analysis started"
        }

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


@router.get("/regions", response_model=List[RegionalData])
async def get_regions():
    """
    Get regional summary data
    """
    try:
        # This would call your function that aggregates country data by region
        # For now, we'll return a placeholder
        regions = [
            {
                "region": "North America",
                "avg_sgm": 4.8,
                "countries": 3,
                "highest_country": "United States",
                "highest_sgm": 5.2,
                "lowest_country": "Canada",
                "lowest_sgm": 2.8
            },
            {
                "region": "Europe",
                "avg_sgm": 3.2,
                "countries": 5,
                "highest_country": "Russia",
                "highest_sgm": 7.3,
                "lowest_country": "Sweden",
                "lowest_sgm": 1.7
            },
            {
                "region": "Asia",
                "avg_sgm": 6.1,
                "countries": 6,
                "highest_country": "China",
                "highest_sgm": 7.0,
                "lowest_country": "Japan",
                "lowest_sgm": 3.6
            },
            {
                "region": "Africa",
                "avg_sgm": 5.7,
                "countries": 3,
                "highest_country": "South Africa",
                "highest_sgm": 5.9,
                "lowest_country": "Kenya",
                "lowest_sgm": 5.1
            },
            {
                "region": "South America",
                "avg_sgm": 4.5,
                "countries": 4,
                "highest_country": "Brazil",
                "highest_sgm": 4.7,
                "lowest_country": "Chile",
                "lowest_sgm": 3.9
            }
        ]

        logger.info(f"Retrieved regional summary data for {len(regions)} regions")
        return regions
    except Exception as e:
        logger.error(f"Error fetching regional data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))