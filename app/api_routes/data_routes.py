from fastapi import APIRouter
from pymongo import MongoClient
from dotenv import load_dotenv
import os

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI")

client = MongoClient(MONGO_URI)
db = client["gdelt_db"]
collection = db["conflict_events"]

router = APIRouter()

@router.get("/events")
async def get_conflict_events(limit: int = 10):
    """
    Fetch conflict events from MongoDB.
    """
    events = list(collection.find({}, {"_id": 0}).limit(limit))
    return events