# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr

# COMMAND ----------


match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("man_of_match", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

match_df = spark.read.format("csv").schema(match_schema)\
    .option("header",True)\
        .load("s3://analytics-etl-s3-bucket/ipl-data-till-2017/Match.csv")

#Date transformation
match_df = match_df.withColumn("match_date", expr("""
                                                    coalesce(
                                                        try_to_date(match_date,"MM/dd/yyyy"),
                                                        try_to_date(match_date,"M/dd/yyyy"),
                                                        try_to_date(match_date,"MM/d/yyyy"),
                                                        try_to_date(match_date,"M/d/yyyy")
                                                        )
                                                    """))

# COMMAND ----------

match_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/match")