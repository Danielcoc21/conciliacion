import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"
import uuid
import pandas as pd
import datacompy
import logging
from app.utils.parquet import read_parquet
from app.utils.spark import get_spark_session
from dotenv import load_dotenv
from app.services.report_service import mark_job_started, mark_job_finished, mark_job_failed
from app.utils.azure_blob import upload_file_to_blob, download_blob_to_local

logger = logging.getLogger("reconciliacion2.comparison")
logging.basicConfig(level=logging.INFO)

def compare_data(request, background_tasks):
    try:
        job_id = f"compare-{uuid.uuid4()}"
        logger.info(f"Iniciando comparación. job_id={job_id}")
        mark_job_started(job_id)
        background_tasks.add_task(_compare_job, request, job_id)
        logger.info(f"Tarea de comparación agregada al background. job_id={job_id}")
        return job_id
    except Exception as e:
        logger.error(f"Error al iniciar comparación: {e}", exc_info=True)
        raise

def _ensure_local_parquet(parquet_path, container_name="documentosreconciliacion"):
    import os
    if os.path.exists(parquet_path):
        return parquet_path
    # Si no existe localmente, intenta descargarlo de Azure Blob Storage
    blob_name = os.path.basename(parquet_path)
    local_path = parquet_path
    try:
        download_blob_to_local(blob_name, local_path, container_name=container_name)
        return local_path
    except Exception as e:
        logger.error(f"No se pudo descargar {blob_name} de Azure Blob Storage: {e}")
        raise FileNotFoundError(f"No se encontró el archivo local ni en Azure Blob Storage: {blob_name}")

def _compare_job(request, job_id):
    try:
        logger.info(f"[{job_id}] Cargando variables de entorno...")
        load_dotenv()
        logger.info(f"[{job_id}] Iniciando sesión Spark...")
        spark = get_spark_session()
        logger.info(f"[{job_id}] Leyendo archivos Parquet...")
        logger.debug(f"[{job_id}] Archivo fuente: {request.source_parquet}")
        logger.debug(f"[{job_id}] Archivo destino: {request.target_parquet}")
        # NUEVO: Asegura que los archivos existan localmente, si no los descarga de Azure
        source_parquet_local = _ensure_local_parquet(request.source_parquet)
        target_parquet_local = _ensure_local_parquet(request.target_parquet)
        df1 = pd.read_parquet(source_parquet_local, engine="pyarrow")
        df2 = pd.read_parquet(target_parquet_local, engine="pyarrow")
        logger.info(f"[{job_id}] Archivos leídos correctamente. Filas df1: {len(df1)}, Filas df2: {len(df2)}")
        logger.info(f"[{job_id}] Iniciando comparación con datacompy...")
        compare = datacompy.Compare(
            df1,
            df2,
            join_columns=request.key_columns,
            abs_tol=0.0001,
            rel_tol=0,
            df1_name='original',
            df2_name='new'
        )
        output_dir = "data/output"
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"[{job_id}] Carpeta de salida creada/verificada: {output_dir}")

        # Prefijo para los archivos de esta comparación (id al final)
        def file_name(base):
            return f"{base}_{job_id}"

        # Guardar resumen ejecutivo JSON
        resumen = {
            "fecha_generacion": "",
            "nombre_df1": compare.df1_name,
            "nombre_df2": compare.df2_name,
            "metricas": {
                "total_filas_df1": len(compare.df1),
                "total_filas_df2": len(compare.df2),
                "filas_unicas_df1": len(compare.df1_unq_rows),
                "filas_unicas_df2": len(compare.df2_unq_rows),
                "filas_intersectadas": len(compare.intersect_rows)
            },
            "estructura": {
                "columnas_compartidas": list(compare.intersect_columns()),
                "columnas_unicas_df1": list(compare.df1_unq_columns()),
                "columnas_unicas_df2": list(compare.df2_unq_columns())
            },
            "estado_comparacion": {
                "dataframes_identicos": compare.matches(),
                "tolerancia_absoluta": compare.abs_tol,
                "tolerancia_relativa": compare.rel_tol
            }
        }
        resumen_path = os.path.join(output_dir, f"{file_name('comparacion_resumen')}.json")
        import json
        def _json_default(obj):
            import numpy as np
            if isinstance(obj, np.generic):
                return obj.item()
            # ...puedes agregar más conversiones si es necesario...
            raise TypeError(f"Object of type {obj._class.name_} is not JSON serializable")
        with open(resumen_path, "w") as f:
            json.dump(resumen, f, indent=4, default=_json_default)
        logger.info(f"[{job_id}] Resumen ejecutivo guardado en: {resumen_path}")
        # Subir a Azure Blob Storage
        resumen_blob_url = upload_file_to_blob(resumen_path, container_name="resultadoscomparacion", job_id=job_id)
        logger.info(f"[{job_id}] Resumen ejecutivo subido a Azure Blob Storage: {resumen_blob_url}")

        # Guardar reporte detallado TXT
        detallado_path = os.path.join(output_dir, f"{file_name('comparacion_reporte_detallado')}.txt")
        with open(detallado_path, "w", encoding="utf-8") as f:
            f.write("REPORTE DETALLADO DE COMPARACIÓN DE DATAFRAMES\n")
            from datetime import datetime
            f.write(f"Fecha: {datetime.now()}\n\n")
            # 1. INFORMACIÓN GENERAL
            f.write("1. INFORMACIÓN GENERAL\n")
            f.write(f"- DataFrame 1 (Original): {compare.df1_name}\n")
            f.write(f"- DataFrame 2 (Nuevo): {compare.df2_name}\n")
            f.write(f"- Total de filas en DataFrame 1: {len(compare.df1)}\n")
            f.write(f"- Total de filas en DataFrame 2: {len(compare.df2)}\n")
            f.write(f"- ¿Son DataFrames idénticos? {compare.matches()}\n\n")
            # 2. ANÁLISIS DE COLUMNAS
            f.write("2. ANÁLISIS DE COLUMNAS\n")
            f.write("Columnas compartidas:\n")
            for col in compare.intersect_columns():
                f.write(f"  - {col}\n")
            f.write("\nColumnas únicas en DataFrame 1:\n")
            for col in compare.df1_unq_columns():
                f.write(f"  - {col}\n")
            f.write("\nColumnas únicas en DataFrame 2:\n")
            for col in compare.df2_unq_columns():
                f.write(f"  - {col}\n")
            f.write("\n")
            # 3. ANÁLISIS DE FILAS
            f.write("3. ANÁLISIS DE FILAS\n")
            f.write(f"Filas únicas en DataFrame 1: {len(compare.df1_unq_rows)}\n")
            f.write(f"Filas únicas en DataFrame 2: {len(compare.df2_unq_rows)}\n\n")
            # 4. FILAS INTERSECTADAS
            f.write("4. FILAS INTERSECTADAS\n")
            f.write(f"Total de filas intersectadas: {len(compare.intersect_rows)}\n")
            f.write("Muestra de filas intersectadas (primeras 10):\n")
            f.write(str(compare.intersect_rows.head(10)))
            f.write("\n\n")
            # 5. DETALLES DE DISCREPANCIAS
            if not compare.matches():
                f.write("5. DETALLES DE DISCREPANCIAS\n")
                f.write(compare.report())
        logger.info(f"[{job_id}] Reporte detallado guardado en: {detallado_path}")
        # Subir a Azure Blob Storage
        detallado_blob_url = upload_file_to_blob(detallado_path, container_name="resultadoscomparacion", job_id=job_id)
        logger.info(f"[{job_id}] Reporte detallado subido a Azure Blob Storage: {detallado_blob_url}")

        # Guardar CSV de filas únicas e intersectadas
        csv_params = {
            'index': False,
            'encoding': 'utf-8-sig',
            'escapechar': '\\',
            'doublequote': True,
            'sep': ',',
            'quoting': 1  # csv.QUOTE_ALL
        }
        if not compare.df1_unq_rows.empty:
            path = os.path.join(output_dir, f"{file_name('comparacion_filas_unicas_df1')}.csv")
            compare.df1_unq_rows.to_csv(path, **csv_params)
            logger.info(f"[{job_id}] Filas únicas df1 guardadas en: {path}")
            # Subir a Azure Blob Storage
            df1_blob_url = upload_file_to_blob(path, container_name="resultadoscomparacion", job_id=job_id)
            logger.info(f"[{job_id}] Filas únicas df1 subidas a Azure Blob Storage: {df1_blob_url}")
        if not compare.df2_unq_rows.empty:
            path = os.path.join(output_dir, f"{file_name('comparacion_filas_unicas_df2')}.csv")
            compare.df2_unq_rows.to_csv(path, **csv_params)
            logger.info(f"[{job_id}] Filas únicas df2 guardadas en: {path}")
            # Subir a Azure Blob Storage
            df2_blob_url = upload_file_to_blob(path, container_name="resultadoscomparacion", job_id=job_id)
            logger.info(f"[{job_id}] Filas únicas df2 subidas a Azure Blob Storage: {df2_blob_url}")
        if not compare.intersect_rows.empty:
            path = os.path.join(output_dir, f"{file_name('comparacion_filas_intersectadas')}.csv")
            compare.intersect_rows.to_csv(path, **csv_params)
            logger.info(f"[{job_id}] Filas intersectadas guardadas en: {path}")
            # Subir a Azure Blob Storage
            intersect_blob_url = upload_file_to_blob(path, container_name="resultadoscomparacion", job_id=job_id)
            logger.info(f"[{job_id}] Filas intersectadas subidas a Azure Blob Storage: {intersect_blob_url}")

        logger.info(f"[{job_id}] Comparación completada y reportes generados.")
        mark_job_finished(job_id, resumen_path)
    except Exception as e:
        logger.error(f"[{job_id}] Error en comparación: {e}", exc_info=True)
        mark_job_failed(job_id, "Error en comparación (detalles internos ocultos por seguridad)")
        with open(f"data/output/{job_id}_error.txt", "a") as f:
            f.write("Error interno\n")