import os
import json
import logging
import sqlite3
from datetime import datetime

# Servicio para registrar, actualizar y consultar el estado y resultado de los procesos (extracción, comparación).
# Usa una base de datos SQLite para persistir el estado de los jobs.

logger = logging.getLogger("reconciliacion2.report")
logging.basicConfig(level=logging.INFO)

DB_PATH = "data/output/jobs.db"

def _init_db():
    # Crea la base de datos y la tabla de jobs si no existen.
    os.makedirs("data/output", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT,
            progress REAL,
            result_path TEXT,
            error TEXT,
            updated_at TEXT
        )
    """)
    conn.commit()
    conn.close()

def _update_job(job_id, status, progress, result_path=None, error=None):
    # Inserta o actualiza el estado de un job en la base de datos.
    _init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        INSERT INTO jobs (job_id, status, progress, result_path, error, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(job_id) DO UPDATE SET
            status=excluded.status,
            progress=excluded.progress,
            result_path=excluded.result_path,
            error=excluded.error,
            updated_at=excluded.updated_at
    """, (job_id, status, progress, result_path, error, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()

def _get_job(job_id):
    # Recupera la información de un job por su ID.
    _init_db()
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT job_id, status, progress, result_path, error, updated_at FROM jobs WHERE job_id = ?", (job_id,))
    row = c.fetchone()
    conn.close()
    if row:
        return {
            "job_id": row[0],
            "status": row[1],
            "progress": row[2],
            "result_path": row[3],
            "error": row[4],
            "updated_at": row[5]
        }
    return None

def get_status(job_id):
    # Devuelve el estado y progreso de un job.
    try:
        job = _get_job(job_id)
        if job:
            logger.info(f"Estado consultado: job_id={job_id} status={job['status']}")
            return {
                "job_id": job_id,
                "status": job["status"],
                "progress": job["progress"]
            }
        else:
            logger.warning(f"Estado consultado: job_id={job_id} no encontrado")
            return {"job_id": job_id, "status": "not_found", "progress": 0.0}
    except Exception as e:
        logger.error(f"Error consultando estado de job_id={job_id}: {e}", exc_info=True)
        return {"job_id": job_id, "status": "error", "progress": 0.0}

def get_result(job_id):
    # Devuelve el resultado de un job (archivo JSON, error o información sobre el directorio Parquet).
    try:
        job = _get_job(job_id)
        if job and job["result_path"]:
            if os.path.isdir(job["result_path"]):
                # Si el resultado es un directorio Parquet, informa al usuario.
                logger.info(f"Resultado consultado: job_id={job_id} es un directorio Parquet")
                return {
                    "job_id": job_id,
                    "result": {
                        "parquet_dir": job["result_path"],
                        "message": "El resultado es un directorio Parquet. Usa herramientas compatibles para leerlo."
                    }
                }
            if os.path.exists(job["result_path"]):
                with open(job["result_path"], "r") as f:
                    result = json.load(f)
                logger.info(f"Resultado consultado: job_id={job_id} OK")
                return {"job_id": job_id, "result": result}
        if job and job["error"]:
            logger.warning(f"Resultado consultado: job_id={job_id} con error")
            return {"job_id": job_id, "result": {"error": job["error"]}}
        else:
            logger.warning(f"Resultado consultado: job_id={job_id} no encontrado")
            return {"job_id": job_id, "result": {}}
    except Exception as e:
        logger.error(f"Error consultando resultado de job_id={job_id}: {e}", exc_info=True)
        return {"job_id": job_id, "result": {}}

# Funciones para marcar el estado de los jobs desde otros servicios.
def mark_job_started(job_id):
    # Marca un job como iniciado.
    _update_job(job_id, status="in_progress", progress=0.0)

def mark_job_finished(job_id, result_path):
    # Marca un job como finalizado exitosamente y guarda la ruta del resultado.
    _update_job(job_id, status="finished", progress=1.0, result_path=result_path)

def mark_job_failed(job_id, error):
    # Marca un job como fallido y guarda el mensaje de error.
    _update_job(job_id, status="failed", progress=1.0, error=error)
