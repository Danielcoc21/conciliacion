from pydantic import BaseModel

class ComparisonRequest(BaseModel):
    source_parquet: str
    target_parquet: str
    key_columns: list[str]

class ComparisonResponse(BaseModel):
    status: str
    message: str
    job_id: str = None
