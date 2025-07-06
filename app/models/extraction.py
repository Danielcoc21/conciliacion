from pydantic import BaseModel
from typing import Optional

class ExtractionRequest(BaseModel):
    source_config: Optional[dict] = None
    target_config: Optional[dict] = None
    file_name_source: Optional[str] = None
    file_name_target: Optional[str] = None

class ExtractionResponse(BaseModel):
    status: str
    message: str
    job_id: str = None
