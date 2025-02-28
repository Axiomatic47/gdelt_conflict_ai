from fastapi import APIRouter
from app.services.gdelt_service import fetch_gdelt_news

router = APIRouter()

@router.get("/gdelt/news/")
def get_gdelt_news(query: str, max_records: int = 10):
    """API Endpoint to fetch real-time news from GDELT."""
    try:
        data = fetch_gdelt_news(query, max_records)
        return {"status": "success", "data": data}
    except Exception as e:
        return {"status": "error", "message": str(e)}
