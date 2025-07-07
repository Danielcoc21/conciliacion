import pyodbc
import os
import random
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    server = os.getenv("AZURE_SQL_SERVER")
    database = os.getenv("AZURE_SQL_DATABASE")
    username = os.getenv("AZURE_SQL_USERNAME")
    password = os.getenv("AZURE_SQL_PASSWORD")
    driver = os.getenv("AZURE_SQL_DRIVER", "ODBC Driver 17 for SQL Server")

    connection_string = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password}"
    )
    return pyodbc.connect(connection_string)

def insert_ejecucion(id_conciliacion, resultado_comparacion, estado_ejecucion, fecha_ejecucion=None):
    """
    Inserta un registro en la tabla ejecuciones.
    """
    conn = get_connection()
    cursor = conn.cursor()
    if fecha_ejecucion is None:
        fecha_ejecucion = datetime.now()
    query = """
        INSERT INTO ejecuciones (id_conciliacion, resultado_comparacion, estado_ejecucion, fecha_ejecucion)
        VALUES (?, ?, ?, ?)
    """
    cursor.execute(query, id_conciliacion, resultado_comparacion, estado_ejecucion, fecha_ejecucion)
    conn.commit()
    cursor.close()
    conn.close()

def generar_id_conciliacion():
    """
    Genera un id aleatorio para id_conciliacion.
    """
    return random.randint(100000, 999999)

def test_connection():
    """
    Prueba la conexión a la base de datos. Retorna True si la conexión es exitosa, False si falla.
    """
    try:
        conn = get_connection()
        conn.close()
        return True
    except Exception as e:
        print(f"Error de conexión: {e}")
        return False
