# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Powerplay Bowler Insights
powerplay_bowler_insights = spark.sql("""
                                    SELECT
                                        bowler_name,
                                        SUM(runs_Scored + bowler_extras) AS total_runs_conceded,
                                        SUM(CASE WHEN bowler_wicket THEN 1 ELSE 0 END) AS wickets,
                                        COUNT(ball_id) AS balls_bowled,
                                        ROUND(
                                            SUM(runs_scored + bowler_extras) * 6 / NULLIF(COUNT(ball_id),0)
                                            ,2
                                        ) AS economy_rate
                                    FROM    silver_level_ipl_data
                                    WHERE   over_id <= 6
                                    GROUP BY bowler_name
                                    HAVING COUNT(ball_id) >= 60
                                    ORDER BY wickets DESC, economy_rate ASC
                                      """)

assert powerplay_bowler_insights.count() > 0, "Powerplay bowler insights dataset is empty"


# COMMAND ----------

powerplay_bowler_insights.write.format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/powerplay-bowler-insights/")