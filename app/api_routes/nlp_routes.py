from fastapi import APIRouter
from app.api_services.nlp_service import analyze_text

router = APIRouter()

@router.post("/analyze")
async def analyze_nlp(text: str):
    """
    API endpoint for NLP sentiment analysis.
    """
    return analyze_text(text)