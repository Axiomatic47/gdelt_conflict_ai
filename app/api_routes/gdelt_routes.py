from fastapi import APIRouter
from app.api_services.gdelt_service import fetch_gdelt_news

router = APIRouter()

@router.get("/news", summary="Fetch GDELT News", tags=["GDELT"])
async def get_gdelt_news():
    return fetch_gdelt_news()