# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import expr

# COMMAND ----------

# Schema setup and Data Load for Player Match file

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("player_match_key", DecimalType(18, 2), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", StringType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opponent_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_man_of_the_match", IntegerType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("is_players_team_won", IntegerType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opponent_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opponent_keeper", StringType(), True)
])

player_match_df = spark.read.format("csv").schema(player_match_schema)\
    .option("header",True)\
        .load("s3://analytics-etl-s3-bucket/ipl-data-till-2017/Player_match.csv")

#Date Transformation
player_match_df = player_match_df.withColumn("dob", 
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

player_match_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/player_match")