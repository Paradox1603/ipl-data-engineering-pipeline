# Databricks notebook source
ball_by_ball_df = spark.read.format("parquet").option("inferSchema",True).option("header",True)\
    .load("s3://analytics-etl-s3-bucket/bronze-level/ball_by_ball")

match_df = spark.read.format("parquet").option("inferSchema",True).option("header",True)\
    .load("s3://analytics-etl-s3-bucket/bronze-level/match")

player_match_df = spark.read.format("parquet").option("inferSchema",True).option("header",True)\
    .load("s3://analytics-etl-s3-bucket/bronze-level/player_match")

player_df = spark.read.format("parquet").option("inferSchema",True).option("header",True)\
    .load("s3://analytics-etl-s3-bucket/bronze-level/player")        

team_df = spark.read.format("parquet").option("inferSchema",True).option("header",True)\
    .load("s3://analytics-etl-s3-bucket/bronze-level/team")   

# COMMAND ----------

# Writing data and storing it back as bronze-level in S3 bucket
assert ball_by_ball_df.count() > 0, "ball_by_ball_df is empty!"
print("Writing ball_by_ball to Bronze layer...")
ball_by_ball_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/ball_by_ball")

assert match_df.count() > 0, "match_df is empty!"
print("Writing Match to Bronze layer...")
match_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/match")

assert player_match_df.count() > 0, "player_match_df is empty!"
print("Writing PLayer Match to Bronze layer...")
player_match_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/player_match")

assert player_df.count() > 0, "player_df is empty!"
print("Writing Player to Bronze layer...")
player_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/player")

assert team_df.count() > 0, "team_df is empty!"
print("Writing Team to Bronze layer...")
team_df.write.mode("overwrite").parquet("s3://analytics-etl-s3-bucket/bronze-level/team")

# COMMAND ----------

dbutils.notebook.exit("Bronze Layer is SUCCESS!")