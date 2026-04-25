# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Batsmen Performance
batsmen_insights = spark.sql("""
                             SELECT
                                batsman_name,
                                SUM(runs_scored) AS total_runs,
                                COUNT(ball_id) AS balls_faced,
                                ROUND(
                                    (SUM(runs_scored) / COUNT(ball_id) * 100)
                                    ,2) AS strike_rate
                             FROM   silver_level_ipl_data
                             WHERE  lower(extra_type) = 'no extras'
                             GROUP BY batsman_name
                             ORDER BY total_runs DESC
                             """)
                             
assert batsmen_insights.count() > 0, "Batsman insights dataset is empty"


# COMMAND ----------

batsmen_insights.write.format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/batsmen-insights/")