from pydantic import BaseModel

class ReportStatusResponse(BaseModel):
    job_id: str
    status: str
    progress: float

class ReportResultResponse(BaseModel):
    job_id: str
    result: dict
