# Databricks notebook source
from pyspark.sql.functions import col, cast, when, expr, regexp_replace, lower, year, current_date
from pyspark.sql.functions import broadcast

# COMMAND ----------

# Reading bronze level data from S3

ball_by_ball_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/bronze-level/ball_by_ball/")
match_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/bronze-level/match/")
player_match_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/bronze-level/player_match/")
player_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/bronze-level/player/")
team_df = spark.read.format("parquet").load("s3://analytics-etl-s3-bucket/bronze-level/team/")

# COMMAND ----------

#Enriching the DataFrames

ball_by_ball_df = ball_by_ball_df.withColumn("caught",col("caught").cast("boolean"))\
        .withColumn("bowled",col("bowled").cast("boolean"))\
            .withColumn("run_out",col("run_out").cast("boolean"))\
                .withColumn("lbw",col("lbw").cast("boolean"))\
                    .withColumn("retired_hurt",col("retired_hurt").cast("boolean"))\
                        .withColumn("stumped",col("stumped").cast("boolean"))\
                            .withColumn("caught_and_bowled",col("caught_and_bowled").cast("boolean"))\
                                .withColumn("hit_wicket",col("hit_wicket").cast("boolean"))\
                                    .withColumn("obstructing_field",col("obstructing_field").cast("boolean"))\
                                        .withColumn("bowler_wicket",col("bowler_wicket").cast("boolean"))\
                                            .withColumn("keeper_catch",col("keeper_catch").cast("boolean"))

ball_by_ball_df = ball_by_ball_df.withColumn(
    "total_runs",
    col("runs_scored") + col("extra_runs")
)

ball_by_ball_df = ball_by_ball_df.withColumn(
    "is_wicket",
    col("bowler_wicket").cast("boolean")
)


# COMMAND ----------

# DBTITLE 1,Transformation on player_df
#Handle missing values in 'Batting_hand' and 'bowling_skill' columns with a default 'unknown'
player_df = player_df.fillna("unknown",subset=["batting_hand","bowling_skill"])

#Categorizing players based on their batting and bowlingstyle
player_df = player_df.withColumn("batting_style",
                                 when (col("batting_hand").contains("Left"), "Left Handed")
                                 .when (col("batting_hand").contains("Right"), "Right Handed")
                                 .otherwise("Unknown"))

player_df = player_df.withColumn("bowling_style",
                                 when (col("bowling_skill").contains("Left"), "Left Handed")
                                 .when (col("bowling_skill").contains("Right"), "Right Handed")
                                 .otherwise("Unknown"))

# COMMAND ----------


player_match_df = player_match_df.withColumn("is_man_of_the_match",col("is_man_of_the_match").cast("boolean"))\
                    .withColumn("is_players_team_won",col("is_players_team_won").cast("boolean"))

#Add a 'veteran_status' column based on player age
player_match_df = player_match_df.withColumn(
    "veteran_status",
    when (col("age_as_on_match") >= 35, "Veteran")
    .otherwise("Non-Veteran")
)

#Dynamic column to calculate years since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

# COMMAND ----------

#Joining the DataFrame

b = ball_by_ball_df.alias("b")
m = match_df.alias("m")

silver_df = b\
    .join(
        m,
        col("b.match_id") == col("m.match_id"),
        "inner"
        ).select(
            "b.*",
            col("m.season_year"),
            col("m.team1"),
            col("m.team2"),
            col("m.toss_winner"),
            col("m.match_winner"),
            col("m.toss_name"),
            col("m.win_margin"),
            col("m.win_type")
            )

p_batsman = player_df.alias("p_batsman")
p_bowler = player_df.alias("p_bowler")

silver_df = silver_df.alias("s") \
    .join(
        broadcast(p_batsman),
        on = col("s.striker") == col("p_batsman.player_id"),
        how= "left"
        )\
        .join(
            broadcast(p_bowler),
            on = col("s.bowler") == col("p_bowler.player_id"),
            how= "left"
            )

batting_team = team_df.alias("batting_team")
bowling_team = team_df.alias("bowling_team")

silver_df = silver_df \
    .join(
        broadcast(batting_team),
        on = col("s.team1") == col("batting_team.team_name"),
        how= "left"
        )\
        .join(
            broadcast(bowling_team),
            on = col("s.team2") == col("bowling_team.team_name"),
            how= "left"
            )

# COMMAND ----------

silver_df = silver_df.select("s.*",
                            col("p_batsman.player_name").alias("batsman_name"),
                            col("p_bowler.player_name").alias("bowler_name"),
                            col("batting_team.team_name").alias("batting_team"),
                            col("bowling_team.team_name").alias("bowling_team")
                            )

original_count = ball_by_ball_df.count()
silver_count = silver_df.count()

assert silver_count >= original_count, "Data loss detected after joins!"


# COMMAND ----------

silver_df.write.mode("overwrite").partitionBy("season_year").format("parquet").save("s3://analytics-etl-s3-bucket/silver-layer/")

# COMMAND ----------

dbutils.notebook.exit("Silver Layer is SUCCESS!")