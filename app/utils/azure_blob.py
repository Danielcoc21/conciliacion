import os
from azure.storage.blob import BlobServiceClient
import shutil
import tempfile
import logging
import glob

logger = logging.getLogger("reconciliacion2.azure_blob")

def upload_file_to_blob(local_path, blob_name=None, container_name=None, job_id=None):
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    AZURE_BLOB_CONTAINER_NAME = container_name or os.getenv("AZURE_BLOB_CONTAINER_NAME")

    if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY or not AZURE_BLOB_CONTAINER_NAME:
        logger.error("Faltan credenciales/configuración de Azure Blob Storage.")
        raise ValueError("Faltan credenciales/configuración de Azure Blob Storage.")

    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT_NAME};"
        f"AccountKey={AZURE_STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)

    # Si es un directorio Parquet, busca el primer archivo part-*.parquet y súbelo
    path_to_upload = local_path
    if os.path.isdir(local_path):
        parquet_files = glob.glob(os.path.join(local_path, "part-*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"No se encontró ningún archivo part-*.parquet en {local_path}")
        path_to_upload = parquet_files[0]
        if not blob_name:
            # Usa el nombre del directorio + job_id + extensión .parquet
            base_name = os.path.basename(local_path.rstrip("/\\"))
            blob_name = f"{base_name}_{job_id}.parquet" if job_id else f"{base_name}.parquet"
    else:
        if not blob_name:
            blob_name = os.path.basename(local_path)

    blob_client = blob_service_client.get_blob_client(
        container=AZURE_BLOB_CONTAINER_NAME,
        blob=blob_name
    )
    try:
        with open(path_to_upload, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info(f"Archivo subido correctamente a Azure Blob Storage: {blob_name}")
    except Exception as e:
        logger.error(f"Error subiendo archivo a Azure Blob Storage: {e}", exc_info=True)
        raise

    return f"https://{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_BLOB_CONTAINER_NAME}/{blob_name}"

def download_blob_to_local(blob_name, local_path, container_name=None):
    AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
    AZURE_STORAGE_ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
    AZURE_BLOB_CONTAINER_NAME = container_name or os.getenv("AZURE_BLOB_CONTAINER_NAME")
    if not AZURE_STORAGE_ACCOUNT_NAME or not AZURE_STORAGE_ACCOUNT_KEY or not AZURE_BLOB_CONTAINER_NAME:
        raise ValueError("Faltan credenciales/configuración de Azure Blob Storage.")
    conn_str = (
        f"DefaultEndpointsProtocol=https;"
        f"AccountName={AZURE_STORAGE_ACCOUNT_NAME};"
        f"AccountKey={AZURE_STORAGE_ACCOUNT_KEY};"
        f"EndpointSuffix=core.windows.net"
    )
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service_client.get_blob_client(container=AZURE_BLOB_CONTAINER_NAME, blob=blob_name)
    with open(local_path, "wb") as f:
        download_stream = blob_client.download_blob()
        f.write(download_stream.readall())
    logger.info(f"Archivo descargado de Azure Blob Storage: {blob_name} -> {local_path}")
