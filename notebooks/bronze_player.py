# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import expr

# COMMAND ----------

# Schema setup and Data Load for Player csv file

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df = spark.read.format("csv").schema(player_schema)\
    .option("header",True)\
        .load("s3://analytics-etl-s3-bucket/ipl-data-till-2017/Player.csv")

#Date Transformation
player_df = player_df.withColumn("dob",
                                    expr("""
                                             coalesce(
                                                try_to_date(dob, "MM/dd/yyyy"),
                                                try_to_date(dob, "M/d/yyyy"),
                                                try_to_date(dob, "M/dd/yyyy"),
                                                try_to_date(dob, "MM/d/yyyy")
                                                    )
                                        """
                                        )
                                 )

# COMMAND ----------

player_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/player")