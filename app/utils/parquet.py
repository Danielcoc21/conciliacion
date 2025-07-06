def save_parquet(df, path):
    df.write.mode("overwrite").parquet(path)

def read_parquet(spark, path):
    return spark.read.parquet(path)
