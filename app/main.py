import sys
import os

# Ensure project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Import API routes
from app.api_routes import sgm_routes
from app.acled_routes import router as acled_router
# Import other routes as needed

# Initialize FastAPI app
app = FastAPI(
    title="GDELT & ACLED Analysis API",
    description="API for fetching GDELT and ACLED data, processing NLP data, and retrieving conflict event insights.",
    version="1.0.0",
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For development - change to your frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API endpoints
app.include_router(sgm_routes.router, prefix="/sgm", tags=["SGM"])
app.include_router(acled_router, prefix="/acled", tags=["ACLED"])
# Include other routers here

# Debug endpoint to list all routes
@app.get("/debug")
async def debug():
    """Debug endpoint to check if FastAPI is working"""
    return {
        "status": "ok",
        "routes": [
            {"path": route.path, "name": route.name}
            for route in app.routes
        ]
    }

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to the GDELT & ACLED Analysis API 🚀"}

# Run Uvicorn server when executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="127.0.0.1", port=4041, reload=True)