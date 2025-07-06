from fastapi import APIRouter, Query
import os
from azure.storage.blob import BlobServiceClient

router = APIRouter()

@router.get("/", summary="Lista los archivos de un contenedor de Azure Blob Storage")
def list_blobs(container_name: str = Query(..., description="Nombre del contenedor")):
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY:
        return {"error": "Faltan credenciales/configuración de Azure Blob Storage."}
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT_NAME};"
        f"AccountKey={AZURE_STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)
    try:
        blobs = [blob.name for blob in container_client.list_blobs()]
        return {"container": container_name, "blobs": blobs}
    except Exception as e:
        return {"error": str(e)}
