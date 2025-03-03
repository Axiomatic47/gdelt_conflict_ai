from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import sys
import os

# Add appropriate core directories to path to import existing functions
sys.path.append(os.path.join(os.path.dirname(__file__), '../../core'))

router = APIRouter(
    prefix="/nlp",
    tags=["nlp-analysis"],
    responses={404: {"description": "Not found"}},
)

class NlpAnalysisData(BaseModel):
    country: str
    sentiment_score: float
    top_themes: List[str]
    related_countries: List[str]
    entity_analysis: Dict[str, float]

@router.get("/results", response_model=List[NlpAnalysisData])
async def get_nlp_results():
    """
    Get NLP analysis results
    """
    try:
        # This would call your function that retrieves NLP analysis results
        # For now, we'll return placeholder data
        results = [
            {
                "country": "United States",
                "sentiment_score": -0.25,
                "top_themes": ["POLITICS", "MILITARY", "ECONOMY"],
                "related_countries": ["China", "Russia", "Mexico"],
                "entity_analysis": {
                    "Government": 0.45,
                    "Military": 0.32,
                    "Economy": 0.23
                }
            },
            {
                "country": "China",
                "sentiment_score": -0.18,
                "top_themes": ["TRADE", "MILITARY", "GOVERNMENT"],
                "related_countries": ["United States", "Russia", "Japan"],
                "entity_analysis": {
                    "Government": 0.52,
                    "Economy": 0.38,
                    "Military": 0.10
                }
            }
            # Add more results as needed
        ]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))