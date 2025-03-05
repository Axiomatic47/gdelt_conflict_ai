"""
Main FastAPI Application with Improved Error Handling
"""
import sys
import os
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# Ensure project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import all API routes
try:
    from app.api_routes.sgm_routes import router as sgm_router
    from app.api_routes.gdelt_routes import router as gdelt_router
    from app.api_routes.acled_routes import router as acled_router
    from app.api_routes.nlp_routes import router as nlp_router
    from app.api_routes.events_routes import router as events_router
    from app.api_routes.data_routes import router as data_router
except ImportError as e:
    logger.error(f"Error importing route modules: {str(e)}")
    # For production, we should handle missing routes more gracefully
    raise

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


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Global exception handler to catch unhandled exceptions and provide friendlier responses
    """
    error_msg = f"Unhandled error: {str(exc)}"
    logger.error(error_msg)
    logger.exception(exc)  # Log full traceback

    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error. Please try again later."}
    )


# Include API endpoints with appropriate prefixes
app.include_router(sgm_router, prefix="/sgm", tags=["SGM"])
app.include_router(gdelt_router, prefix="/gdelt", tags=["GDELT"])
app.include_router(acled_router, prefix="/acled", tags=["ACLED"])
app.include_router(nlp_router, prefix="/nlp", tags=["NLP"])
app.include_router(events_router, prefix="/events", tags=["Events"])
app.include_router(data_router, prefix="/data", tags=["Data"])


# Debug endpoint to list all routes
@app.get("/debug")
async def debug():
    """Debug endpoint to check if FastAPI is working"""
    try:
        # Get all registered routes with their path and name
        routes = [
            {"path": route.path, "name": route.name}
            for route in app.routes
        ]
        return {
            "status": "ok",
            "routes": routes
        }
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }


# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Welcome to the GDELT & ACLED Analysis API",
        "version": "1.0.0",
        "endpoints": [
            "/sgm - Supremacism Global Metrics",
            "/gdelt - GDELT conflict events",
            "/acled - ACLED conflict events",
            "/events - Combined conflict events",
            "/nlp - NLP analysis results",
            "/data - General analysis data"
        ],
        "docs": "/docs",  # Link to Swagger documentation
        "debug": "/debug"  # Link to debug endpoint
    }


# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring"""
    return {"status": "healthy"}


# Run Uvicorn server when executed directly
if __name__ == "__main__":
    import uvicorn

    # Get port from environment or use default
    port = int(os.getenv("PORT", "4041"))

    # Log startup message
    logger.info(f"Starting API server on port {port}")
    logger.info("Press CTRL+C to stop")

    # Start server
    uvicorn.run("app.main:app", host="127.0.0.1", port=port, reload=True)