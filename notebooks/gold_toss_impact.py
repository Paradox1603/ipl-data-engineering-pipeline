# Databricks notebook source
# Reading silver level data from S3

silver_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/silver-layer/")
silver_df.createOrReplaceTempView("silver_level_ipl_data")

# COMMAND ----------

# DBTITLE 1,Toss impact
#Toss impact on individual matches
toss_impact = spark.sql("""
                        SELECT  
                            match_id,
                            MAX(season_year) AS season_year,
                            MAX(toss_winner) AS toss_winner,
                            MAX(match_winner) AS match_winner,
                            MAX(toss_name) AS toss_name,
                            MAX(CASE WHEN toss_winner = match_winner THEN 'Win' ELSE 'Loss' END) AS match_outcome
                        FROM    silver_level_ipl_data
                        WHERE   toss_name IS NOT NULL
                        GROUP BY match_id
                        """)

assert toss_impact.count() > 0, "Toss impact dataset is empty"


# COMMAND ----------

toss_impact.write.partitionBy("season_year").format("parquet").mode("overwrite").save("s3://analytics-etl-s3-bucket/gold-layer/toss-impact/")