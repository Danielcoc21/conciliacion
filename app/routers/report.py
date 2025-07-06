from fastapi import APIRouter
from app.models.report import ReportStatusResponse, ReportResultResponse
from app.services.report_service import get_status, get_result

router = APIRouter()

@router.get("/status/{job_id}", response_model=ReportStatusResponse)
def check_status(job_id: str):
    return get_status(job_id)

@router.get("/result/{job_id}", response_model=ReportResultResponse)
def get_report_result(job_id: str):
    return get_result(job_id)
