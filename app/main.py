from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from app.routers import extraction, comparison, report
from app.routers import azure_blob_list

app = FastAPI(title="Reconciliacion2 Microservice")

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

app.include_router(extraction.router, prefix="/extraction", tags=["Extraction"])
app.include_router(comparison.router, prefix="/comparison", tags=["Comparison"])
app.include_router(report.router, prefix="/report", tags=["Report"])
app.include_router(azure_blob_list.router, prefix="/azure-blobs", tags=["AzureBlob"])