"""
FastAPI routes for ACLED data integration
"""

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
import logging
import uuid
import sys
import os

# Add core directory to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Import ACLED data service
from core.acled_client import fetch_and_store_acled, get_acled_data_for_heatmap

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter()

# Dictionary to store job status (replace with database in production)
acled_jobs = {}

# Background task for running ACLED fetch
def run_acled_fetch_task(job_id: str, days_back: int = 30, limit: int = 500):
    try:
        logger.info(f"Starting ACLED fetch job {job_id}")
        acled_jobs[job_id] = {"status": "running", "progress": 0.1, "message": "Fetching ACLED data..."}

        # Update progress
        acled_jobs[job_id]["progress"] = 0.3
        acled_jobs[job_id]["message"] = "Connecting to ACLED API..."

        # Fetch and store ACLED data
        success = fetch_and_store_acled(days_back=days_back, limit=limit)

        # Update job status based on result
        if success:
            acled_jobs[job_id] = {
                "status": "completed",
                "progress": 1.0,
                "message": f"Successfully fetched and stored ACLED data for the past {days_back} days."
            }
            logger.info(f"Completed ACLED fetch job {job_id}")
        else:
            acled_jobs[job_id] = {
                "status": "failed",
                "progress": 1.0,
                "message": "Failed to fetch or store ACLED data."
            }
            logger.error(f"Failed ACLED fetch job {job_id}")

    except Exception as e:
        logger.error(f"Error in ACLED fetch job {job_id}: {str(e)}")
        acled_jobs[job_id] = {
            "status": "failed",
            "progress": 1.0,
            "message": f"Error: {str(e)}"
        }

@router.get("/events")
async def get_acled_events():
    """
    Get ACLED event data for visualization
    """
    try:
        events = get_acled_data_for_heatmap()
        return {"events": events, "count": len(events)}
    except Exception as e:
        logger.error(f"Error fetching ACLED events: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/fetch")
async def fetch_acled_events(
    background_tasks: BackgroundTasks,
    days_back: int = Query(30, ge=1, le=365),
    limit: int = Query(500, ge=1, le=10000)
):
    """
    Trigger a new ACLED data fetch
    """
    try:
        job_id = str(uuid.uuid4())
        background_tasks.add_task(run_acled_fetch_task, job_id, days_back, limit)

        # Store job status
        acled_jobs[job_id] = {
            "status": "started",
            "progress": 0.0,
            "message": "ACLED data fetch started"
        }

        logger.info(f"Started ACLED fetch job {job_id}")
        return {"jobId": job_id, "status": "started"}
    except Exception as e:
        logger.error(f"Error starting ACLED fetch: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{job_id}")
async def get_acled_fetch_status(job_id: str):
    """
    Get the status of an ACLED fetch job
    """
    try:
        if job_id not in acled_jobs:
            raise HTTPException(status_code=404, detail=f"ACLED fetch job {job_id} not found")

        job_status = acled_jobs[job_id]
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