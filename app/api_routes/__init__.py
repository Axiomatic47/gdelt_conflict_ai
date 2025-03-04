from fastapi import APIRouter
from app.api_routes import sgm_routes, gdelt_routes, nlp_routes
from . import sgm_routes, gdelt_routes, nlp_routes, acled_routes

router = APIRouter(
    prefix="/api",
)

router.include_router(sgm_routes.router)
router.include_router(gdelt_routes.router)
router.include_router(nlp_routes.router)