# Imagen base oficial de Python
FROM python:3.11-slim

# Instala Java (requerido por Spark)
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

# Instala dependencias del sistema para PySpark y pandas
RUN apt-get update && apt-get install -y gcc g++ build-essential && rm -rf /var/lib/apt/lists/*

# Variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

# Crea directorios de trabajo
WORKDIR /app

# Copia requirements y archivos del proyecto
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto del código fuente y recursos
COPY app/ app/
COPY data/ data/
COPY lib/ lib/
COPY config.toml config.toml

# Copia el driver JDBC (asegúrate de tener el JAR correcto en lib/)
# Por defecto, usa el de SQL Server, pero puedes cambiarlo por el de Oracle si lo necesitas
ENV JDBC_DRIVER_PATH=lib/mssql-jdbc-10.2.0.jre11.jar

# Expone el puerto de FastAPI
EXPOSE 8000

# Comando para lanzar el microservicio
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
