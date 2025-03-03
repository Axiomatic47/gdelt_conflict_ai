import sys
import os

# Ensure project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api_routes import sgm_routes, gdelt_routes, nlp_routes, data_routes

# Initialize FastAPI app
app = FastAPI(
    title="GDELT Conflict Analysis API",
    description="API for fetching GDELT news, processing NLP data, and retrieving SGM (Supremacism-Governance Methodology) data.",
    version="1.0.0",
)

# Enable CORS for frontend requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # For production, restrict this to your actual frontend domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(sgm_routes.router, prefix="/sgm", tags=["SGM"])
app.include_router(gdelt_routes.router, prefix="/gdelt", tags=["GDELT"])
app.include_router(nlp_routes.router, prefix="/nlp", tags=["NLP"])
app.include_router(data_routes.router, prefix="/data", tags=["Data"])

# Root endpoint
@app.get("/")
async def root():
    return {
        "message": "Welcome to the GDELT Conflict Analysis API 🚀",
        "documentation": "/docs",
        "available_endpoints": [
            "/sgm/countries",
            "/sgm/countries/{country_code}",
            "/sgm/regions",
            "/sgm/run-analysis",
            "/gdelt/events",
            "/nlp/results"
        ]
    }

# Run Uvicorn server when executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4041, reload=True)