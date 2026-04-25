# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, to_date, expr, cast

# COMMAND ----------

# Schema setup and Data Load for Teams csv file

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

team_df = spark.read.format("csv").schema(team_schema)\
    .option("header",True)\
        .load("s3://analytics-etl-s3-bucket/ipl-data-till-2017/Team.csv")

# COMMAND ----------

team_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/team")