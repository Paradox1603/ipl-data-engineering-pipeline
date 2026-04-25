# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Top Batsman per Season
top_batsman = spark.sql("""
                        SELECT
                            season_year,
                            batsman_name,
                            total_runs,
                            batting_avg
                        FROM(SELECT
                                total_runs,
                                batsman_name,
                                season_year,
                                batting_avg,
                                ROW_NUMBER() OVER (PARTITION BY agg.season_year ORDER BY total_runs DESC) AS rn
                            FROM(SELECT
                                    SUM(runs_scored) AS total_runs,
                                    batsman_name,
                                    season_year,
                                    SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END) AS no_of_dismissals,
                                    ROUND(
                                        (SUM(runs_scored) / NULLIF(SUM(CASE WHEN is_wicket THEN 1 ELSE 0 END),0) )
                                        ,2) AS batting_avg
                                FROM    silver_level_ipl_data
                                GROUP BY batsman_name, season_year)agg
                            )ranked
                        WHERE rn = 1
                        """)

assert top_batsman.count() > 0, "Top batsman dataset is empty"


# COMMAND ----------

top_batsman.write.format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/top-batsman/")