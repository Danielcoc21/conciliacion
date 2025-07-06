import os
from pyspark.sql import SparkSession

def get_spark_session():
    jdbc_driver_path = os.getenv("JDBC_DRIVER_PATH", "lib/ojdbc8.jar")
    builder = SparkSession.builder.master("local[*]").appName("Reconciliacion2")
    builder = builder.config("spark.hadoop.io.native.lib.available", "false")
    if jdbc_driver_path and os.path.exists(jdbc_driver_path):
        builder = builder.config("spark.jars", jdbc_driver_path)
    return builder.getOrCreate()
