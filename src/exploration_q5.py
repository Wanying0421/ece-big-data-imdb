from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("IMDb Exploration Q5").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

episodes = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("nullValue", r"\N")
    .csv("data/raw/title.episode.tsv.gz")
    .select(
        F.col("tconst").alias("episode_id"),
        F.col("parentTconst").alias("series_id"),
        F.col("seasonNumber").cast("int").alias("season"),
    )
    .dropna(subset=["episode_id", "series_id", "season"])
)

episodes_per_season = episodes.groupBy("series_id", "season").agg(
    F.countDistinct("episode_id").alias("episode_count")
)

eligible_series = (
    episodes_per_season
    .filter(F.col("episode_count") >= 10)
    .groupBy("series_id")
    .agg(F.count("*").alias("qualifying_seasons"))
    .filter(F.col("qualifying_seasons") >= 3)
)

ratings = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("nullValue", r"\N")
    .csv("data/raw/title.ratings.tsv.gz")
    .select(
        F.col("tconst").alias("episode_id"),
        F.col("averageRating").cast("decimal(3,1)").alias("rating"),
    )
    .dropna(subset=["episode_id", "rating"])
)

# Find the last season before joining ratings, since some episodes have no rating.
last_seasons = episodes.groupBy("series_id").agg(
    F.max("season").alias("last_season")
)

series_window = Window.partitionBy("series_id")
rated_episodes = (
    episodes.join(eligible_series, "series_id")
    .join(last_seasons, "series_id")
    .join(ratings, "episode_id")
    .withColumn("best_rating", F.max("rating").over(series_window))
    .withColumn("worst_rating", F.min("rating").over(series_window))
)

matching_series = (
    rated_episodes
    .groupBy("series_id", "qualifying_seasons", "last_season", "best_rating", "worst_rating")
    .agg(
        F.max(
            F.when(
                (F.col("rating") == F.col("best_rating")) & F.col("season").isin(1, 2),
                1,
            ).otherwise(0)
        ).alias("best_in_first_two"),
        F.max(
            F.when(
                (F.col("rating") == F.col("worst_rating"))
                & (F.col("season") == F.col("last_season")),
                1,
            ).otherwise(0)
        ).alias("worst_in_last"),
    )
    .filter((F.col("best_in_first_two") == 1) & (F.col("worst_in_last") == 1))
    .drop("best_in_first_two", "worst_in_last")
)

series_titles = (
    spark.read
    .option("header", True)
    .option("sep", "\t")
    .option("nullValue", r"\N")
    .option("quote", "")
    .csv("data/raw/title.basics.tsv.gz")
    .filter(F.col("titleType") == "tvSeries")
    .select(
        F.col("tconst").alias("series_id"),
        F.col("primaryTitle").alias("series_title"),
    )
)

results = matching_series.join(series_titles, "series_id").select(
    "series_id",
    "series_title",
    "qualifying_seasons",
    "last_season",
    "best_rating",
    "worst_rating",
)

results.orderBy("series_id").show(20, truncate=False)
spark.stop()