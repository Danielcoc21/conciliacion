import os
import uuid
import json
import logging
from app.utils.spark import get_spark_session
from app.utils.parquet import save_parquet
from dotenv import load_dotenv
from app.services.report_service import mark_job_started, mark_job_finished, mark_job_failed
from app.utils.azure_blob import upload_file_to_blob

logger = logging.getLogger("reconciliacion2.extraction")
logging.basicConfig(level=logging.INFO)

def get_valid_fernet_key(raw_key):
    """
    Valida que la clave Fernet sea 32 bytes codificados en base64 url-safe.
    Lanza un error claro si no es válida.
    """
    import base64
    if not raw_key:
        raise ValueError("La clave Fernet está vacía o no definida.")
    try:
        decoded = base64.urlsafe_b64decode(raw_key)
        if len(decoded) != 32:
            raise ValueError
        return raw_key
    except Exception:
        raise ValueError("La clave Fernet debe ser 32 bytes codificados en base64 url-safe. "
                         "Corrige la variable de entorno o el archivo de configuración.")

def decrypt_data(encrypted_data, key):
    from cryptography.fernet import Fernet, InvalidToken
    key = get_valid_fernet_key(key)
    cipher = Fernet(key)
    try:
        return cipher.decrypt(encrypted_data.encode()).decode()
    except InvalidToken:
        raise ValueError("La contraseña no está cifrada correctamente o la clave Fernet es incorrecta.")
    except Exception as e:
        raise ValueError(f"Error descifrando la contraseña: {e}")

def load_json_config(file_name):
    input_dir = "data/input"
    if not file_name or not isinstance(file_name, str):
        raise ValueError("El nombre del archivo de configuración JSON no puede ser None o vacío.")
    file_path = os.path.join(input_dir, file_name)
    with open(file_path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_sql_from_json(param_json):
    # Lógica similar a Json_Parameter.generate_dynamic_sql del monolítico
    if (param_json.get("type", None) == "SQL"):
        query = param_json.get("query", None)
        if isinstance(query, list):
            query = ' '.join(query)
        query = ' '.join(query.split())
        return query
    else:
        columns = param_json.get("columns", [])
        alias = param_json.get("alias", [])
        table_name = param_json.get("table_name", "")
        schema = param_json.get("schema", "")
        filter_conditions = param_json.get("filter", [])
        # Validar que las columnas y alias tengan la misma longitud
        columns_with_alias = [f"{col} AS {al}" for col, al in zip(columns, alias)] if alias else columns
        if schema:
            sql_query = f"SELECT {', '.join(columns_with_alias)} FROM {schema}.{table_name}"
        else:
            sql_query = f"SELECT {', '.join(columns_with_alias)} FROM {table_name}"
        if filter_conditions:
            sql_query += f" WHERE {' AND '.join(filter_conditions)}"
        return sql_query

def extract_data(request, background_tasks):
    try:
        # Permite que el request tenga file_name_source y file_name_target en vez de los dicts completos
        if hasattr(request, "file_name_source") and hasattr(request, "file_name_target"):
            if not request.file_name_source or not request.file_name_target:
                raise ValueError("Debes proporcionar ambos: file_name_source y file_name_target.")
            source_config = load_json_config(request.file_name_source)
            target_config = load_json_config(request.file_name_target)
        else:
            if not request.source_config or not request.target_config:
                raise ValueError("Debes proporcionar ambos: source_config y target_config.")
            source_config = request.source_config
            target_config = request.target_config

        job_id = f"extract-{uuid.uuid4()}"
        mark_job_started(job_id)
        background_tasks.add_task(_extract_job, source_config, target_config, job_id)
        logger.info(f"Extracción iniciada. job_id={job_id}")
        return job_id
    except Exception as e:
        logger.error(f"Error al iniciar extracción: {e}", exc_info=True)
        raise

def _extract_job(source_config, target_config, job_id):
    try:
        load_dotenv()
        spark = get_spark_session()
        for side, config in [("source", source_config), ("target", target_config)]:
            jdbc_url = None
            query = None
            user = config.get("user") or os.getenv("DB_USER")
            password = config.get("password") or os.getenv("DB_PASSWORD")
            encryption_key = os.getenv("DB_ENCRYPTION_KEY")
            if password and encryption_key:
                try:
                    password = decrypt_data(password, encryption_key)
                except Exception as e:
                    logger.error(f"Error descifrando la contraseña para {side}: {e}", exc_info=True)
                    mark_job_failed(job_id, f"Error descifrando la contraseña: {e}")
                    return

            file_name = config.get("file_name", f"{side}_{job_id}")
            output_path = f"data/output/{file_name}.parquet"
            db_type = config.get("type", "").upper()
            if db_type == "TXT":
                # Soporte para extracción desde archivo TXT/CSV
                input_path = config.get("input_path") or config.get("table_name")
                if not input_path:
                    logger.error(f"Falta 'input_path' o 'table_name' para TXT en {side}")
                    mark_job_failed(job_id, f"Falta 'input_path' o 'table_name' para TXT en {side}")
                    return
                delimiter = config.get("delimiter", ",")
                header = config.get("header", True)
                df = spark.read.csv(input_path, sep=delimiter, header=header, inferSchema=True)
                save_parquet(df, output_path)
                logger.info(f"Extracción TXT {side} completada y guardada en {output_path}")
                blob_url = upload_file_to_blob(output_path, job_id=job_id)
                logger.info(f"Archivo {output_path} subido a Azure Blob Storage: {blob_url}")
                if side == "target":
                    mark_job_finished(job_id, blob_url)
                continue  # Salta el resto del ciclo para TXT

            db_name = config.get("data_base_name")
            driver = "oracle.jdbc.driver.OracleDriver"
            if db_name:
                import tomli
                with open("config.toml", "rb") as f:
                    toml_conf = tomli.load(f)
                db_conf = toml_conf["database"][db_name]
                # Detecta si es SQL Server o Oracle
                if "sqlserver" in db_name or "azure_sql" in db_name or "mssql" in db_name:
                    jdbc_url = f"jdbc:sqlserver://{db_conf['host']}:{db_conf['port']};databaseName={db_conf['service_name']}"
                    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                else:
                    jdbc_url = f"jdbc:oracle:thin:@{db_conf['host']}:{db_conf['port']}/{db_conf['service_name']}"

            sql_query = build_sql_from_json(config)
            query = f"({sql_query}) alias"

            try:
                logger.info(f"Iniciando extracción {side}: {output_path}")
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", query) \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", driver) \
                    .load()
                print(df)
                save_parquet(df, output_path)
                logger.info(f"Extracción {side} completada y guardada en {output_path}")

                # Subir a Azure Blob Storage, pasando el job_id
                blob_url = upload_file_to_blob(output_path, job_id=job_id)
                logger.info(f"Archivo {output_path} subido a Azure Blob Storage: {blob_url}")

                # Opcional: eliminar el archivo local después de subirlo
                # os.remove(output_path)

                if side == "target":
                    mark_job_finished(job_id, blob_url)
            except Exception as e:
                logger.error(f"Error extrayendo/parqueteando {side}: {e}", exc_info=True)
                mark_job_failed(job_id, "Error en extracción/parquet (detalles internos ocultos por seguridad)")
                with open(f"data/output/{job_id}_error.txt", "a") as f:
                    f.write(f"{side}: Error interno\n")
    except Exception as e:
        logger.critical(f"Error crítico en extracción: {e}", exc_info=True)
        mark_job_failed(job_id, "Error crítico en extracción (detalles internos ocultos por seguridad)")
        with open(f"data/output/{job_id}_error.txt", "a") as f:
            f.write(f"CRITICAL: Error interno\n")
