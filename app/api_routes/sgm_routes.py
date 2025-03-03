#!/usr/bin/env python3
"""
SGM API Routes - Simplified for GDELT integration
"""

from fastapi import APIRouter, HTTPException
from pymongo import MongoClient
from typing import List, Dict, Any
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

# Initialize router
router = APIRouter()

# Connect to MongoDB
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client["gdelt_db"]
    sgm_collection = db["sgm_scores"]
except Exception as e:
    print(f"Warning: MongoDB connection failed: {str(e)}")
    mongo_client = None


@router.get("/countries", summary="Get all countries with SGM scores")
async def get_countries():
    """Get SGM scores for all countries"""
    if not mongo_client:
        # Return mock data if MongoDB is not available
        return [
            {
                "country": "United States",
                "code": "US",
                "srsD": 4.2,
                "srsI": 6.7,
                "gscs": 5.2,
                "sti": 45,
                "category": "Soft Supremacism",
                "description": "United States exhibits soft supremacism with institutional inequalities despite formal legal equality."
            },
            {
                "country": "China",
                "code": "CN",
                "srsD": 7.1,
                "srsI": 6.8,
                "gscs": 7.0,
                "sti": 75,
                "category": "Structural Supremacism",
                "description": "China demonstrates structural supremacism with notable inequalities at societal and governmental levels."
            },
            {
                "country": "Russia",
                "code": "RU",
                "srsD": 6.9,
                "srsI": 7.8,
                "gscs": 7.3,
                "sti": 80,
                "category": "Structural Supremacism",
                "description": "Russia demonstrates structural supremacism with notable inequalities at societal and governmental levels."
            }
        ]

    try:
        countries = list(sgm_collection.find({}, {"_id": 0}))
        if not countries:
            # If no data in database, return mock data
            return [
                {
                    "country": "United States",
                    "code": "US",
                    "srsD": 4.2,
                    "srsI": 6.7,
                    "gscs": 5.2,
                    "sti": 45,
                    "category": "Soft Supremacism",
                    "description": "United States exhibits soft supremacism with institutional inequalities despite formal legal equality."
                }
            ]
        return countries
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/countries/{country_code}", summary="Get SGM scores for a specific country")
async def get_country(country_code: str):
    """Get detailed SGM data for a specific country"""
    if not mongo_client:
        # Return mock data if MongoDB is not available
        if country_code.upper() == "US":
            return {
                "country": "United States",
                "code": "US",
                "srsD": 4.2,
                "srsI": 6.7,
                "gscs": 5.2,
                "sti": 45,
                "category": "Soft Supremacism",
                "description": "United States exhibits soft supremacism with institutional inequalities despite formal legal equality."
            }
        raise HTTPException(status_code=404, detail=f"Country not found: {country_code}")

    try:
        country = sgm_collection.find_one({"code": country_code.upper()}, {"_id": 0})
        if not country:
            raise HTTPException(status_code=404, detail=f"Country not found: {country_code}")
        return country
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@router.get("/run-analysis", summary="Trigger SGM analysis")
async def run_analysis():
    """Trigger a new SGM analysis run"""
    try:
        from core.sgm_data_service import SGMService
        service = SGMService()
        success = service.run_sgm_analysis()

        if success:
            return {"status": "success", "message": "SGM analysis completed successfully"}
        else:
            return {"status": "error", "message": "SGM analysis encountered issues"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error running SGM analysis: {str(e)}")


@router.get("/test", summary="Test endpoint")
async def test_endpoint():
    """Test endpoint to verify API is working"""
    return {
        "status": "success",
        "message": "SGM API is operational",
        "timestamp": datetime.now().isoformat()
    }