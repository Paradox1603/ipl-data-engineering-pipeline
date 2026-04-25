# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

ball_by_ball_df = spark.read.format("parquet").option("inferSchema",True).option("header",True).\
    load("s3://analytics-etl-s3-bucket/bronze-level/ball_by_ball/")

silver_df = spark.read.format("parquet").option("inferSchema",True).option("header",True).\
    load("s3://analytics-etl-s3-bucket/silver-layer/")

# COMMAND ----------

original_count = ball_by_ball_df.count()
silver_count = silver_df.count()

assert silver_count >= original_count, "Data loss detected after joins!"

# COMMAND ----------

silver_df = silver_df.filter(col("batsman_name").isNotNull())

silver_df.write.mode("overwrite").partitionBy("season_year").format("parquet").save("s3://analytics-etl-s3-bucket/silver-layer/")