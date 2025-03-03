import sys
import os

# ✅ Ensure project root is in the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  # Optional: Enable CORS for frontend access

# ✅ Import API routes
from app.api_routes import gdelt_routes, nlp_routes, data_routes

# ✅ Initialize FastAPI app
app = FastAPI(
    title="GDELT Conflict Analysis API",
    description="API for fetching GDELT news, processing NLP data, and retrieving conflict event insights.",
    version="1.0.0",
)

# ✅ Enable CORS (if needed for frontend requests)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your frontend URL for better security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Include API endpoints
app.include_router(gdelt_routes.router, prefix="/gdelt", tags=["GDELT"])
app.include_router(nlp_routes.router, prefix="/nlp", tags=["NLP"])
app.include_router(data_routes.router, prefix="/data", tags=["Data"])

# ✅ Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to the GDELT Conflict Analysis API 🚀"}

# ✅ Run Uvicorn server when executed directly
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=4041, reload=True)