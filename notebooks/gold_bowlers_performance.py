# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Bowlers Performance
bowler_insights = spark.sql("""
                               SELECT
                                    bowler_name,
                                    SUM(runs_scored + bowler_extras) AS runs_conceded,
                                    SUM(CASE WHEN bowler_wicket THEN 1 ELSE 0 END) AS wickets_taken,
                                    COUNT(ball_id) AS balls_bowled,
                                    ROUND(
                                        COUNT(ball_id) / 6
                                        ,2)
                                        AS overs_bowled,
                                    ROUND(
                                        SUM(runs_scored + bowler_extras) * 6 / (COUNT(ball_id) )
                                        ,2) As economy_rate
                                FROM    silver_level_ipl_data
                                GROUP BY bowler_name
                                HAVING (COUNT(ball_id) / 6) > 10
                                ORDER BY wickets_taken DESC, economy_rate
                               """)

assert bowler_insights.count() > 0, "Bowler insights dataset is empty"


# COMMAND ----------

bowler_insights.write.format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/bowler-insights/")