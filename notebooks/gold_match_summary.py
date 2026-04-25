# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Match Summary
match_insights = spark.sql("""
                           SELECT
                                match_id,
                                MAX(season_year) AS season_year,
                                SUM(runs_scored + extra_runs) AS total_match_runs,
                                MAX(team1) AS team1,
                                MAX(team2) AS team2,
                                (
                                    MAX(match_winner) || 
                                    ' won by ' || 
                                    MAX(win_margin) ||' '|| 
                                    MAX(win_type)
                                ) AS match_result
                            FROM    silver_level_ipl_data
                            WHERE   match_id IS NOT NULL
                            GROUP BY match_id
                            ORDER BY match_id
                           """)

assert match_insights.count() > 0, "Match insights dataset is empty"


# COMMAND ----------

match_insights.write.format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/match-insights/")