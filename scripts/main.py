from fastapi import FastAPI
from app.routes.gdelt_routes import router as gdelt_router

app = FastAPI(title="GDELT API Wrapper")

# Include Routes
app.include_router(gdelt_router, prefix="/api")

@app.get("/")
def root():
    return {"message": "Welcome to the GDELT API Wrapper!"}
