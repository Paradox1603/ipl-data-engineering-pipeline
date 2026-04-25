# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
from pyspark.sql.functions import col, when, to_date, expr, cast

# COMMAND ----------

# Bronze Layer
# Schema setup and Data Load for all the CSV Files

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", IntegerType(), True),
    StructField("bowled", IntegerType(), True),
    StructField("run_out", IntegerType(), True),
    StructField("lbw", IntegerType(), True),
    StructField("retired_hurt", IntegerType(), True),
    StructField("stumped", IntegerType(), True),
    StructField("caught_and_bowled", IntegerType(), True),
    StructField("hit_wicket", IntegerType(), True),
    StructField("obstructing_field", IntegerType(), True),
    StructField("bowler_wicket", IntegerType(), True),
    StructField("match_date", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", IntegerType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", StringType(), True)
])

ball_by_ball_df = spark.read.format("csv").schema(ball_by_ball_schema)\
    .option("header",True)\
        .load("s3://analytics-etl-s3-bucket/ipl-data-till-2017/Ball_By_Ball.csv")

ball_by_ball_df = ball_by_ball_df.withColumn("match_date",to_date("match_date","M/d/yyyy"))

# COMMAND ----------

ball_by_ball_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/ball_by_ball")