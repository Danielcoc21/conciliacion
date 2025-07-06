from fastapi import APIRouter, BackgroundTasks
from app.models.extraction import ExtractionRequest, ExtractionResponse
from app.services.extraction_service import extract_data

router = APIRouter()

@router.post("/", response_model=ExtractionResponse)
def start_extraction(request: ExtractionRequest, background_tasks: BackgroundTasks):
    job_id = extract_data(request, background_tasks)
    return ExtractionResponse(status="started", message="Extraction started", job_id=job_id)
