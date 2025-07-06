from fastapi import APIRouter, BackgroundTasks
from app.models.comparison import ComparisonRequest, ComparisonResponse
from app.services.comparison_service import compare_data
import logging

router = APIRouter()
logger = logging.getLogger("reconciliacion2.routers.comparison")

@router.post("/", response_model=ComparisonResponse)
def start_comparison(request: ComparisonRequest, background_tasks: BackgroundTasks):
    logger.info("Solicitud de comparación recibida")
    job_id = compare_data(request, background_tasks)
    logger.info(f"Comparación iniciada, job_id={job_id}")
    return ComparisonResponse(status="started", message="Comparison started", job_id=job_id)