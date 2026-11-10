"""Q2: top three most popular movie genres for each year between 1980 and 2025.

Definitions (consistent with top10.py)
--------------------------------------
- Movie: titleType == 'movie', isAdult == 0, runtimeMinutes >= 40,
  startYear between 1980 and 2025.
- Popularity of a (year, genre) = sum of numVotes of its movies.
  Movies without a rating count as 0 votes.
- A movie with several genres counts fully in each of its genres.
- Ranking inside a year: total votes desc, then genre name (deterministic ties).

Outputs
-------
- reports/q2_genre_popularity_by_year.csv : every (year, genre) with rank and metrics
  (also the input for Q3)
- reports/q2_top3_genres_by_year.csv      : rank <= 3 only
"""

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BASE_DIR = "/home/jovyan/work"
CLEAN_DIR = os.path.join(BASE_DIR, "data", "parquet_clean")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

MIN_RUNTIME = 40
YEAR_MIN = 1980
YEAR_MAX = 2025
TOP_N = 3


def genre_array(df):
    """Return genres as an array column, whether stored as array or CSV string."""
    if isinstance(df.schema["genres"].dataType, T.ArrayType):
        return F.col("genres")
    return F.split(F.col("genres"), ",")


def main():
    spark = SparkSession.builder.appName("explore_q2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    basics = spark.read.parquet(os.path.join(CLEAN_DIR, "title_basics"))
    ratings = spark.read.parquet(os.path.join(CLEAN_DIR, "title_ratings"))

    movies = (
        basics
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
        .withColumn("startYear", F.col("startYear").cast("int"))
        .filter(F.col("titleType") == "movie")
        .filter(F.col("isAdult").cast("int") == 0)
        .filter(F.col("runtimeMinutes") >= MIN_RUNTIME)
        .filter(F.col("startYear").between(YEAR_MIN, YEAR_MAX))
        .join(
            ratings.select("tconst", "averageRating", "numVotes"),
            on="tconst",
            how="left",
        )
        .withColumn("numVotes", F.coalesce(F.col("numVotes").cast("long"), F.lit(0)))
        .select("tconst", "startYear", "averageRating", "numVotes", "genres")
        .cache()
    )

    # Total votes of distinct movies per year (denominator for vote share).
    year_totals = movies.groupBy("startYear").agg(
        F.sum("numVotes").alias("year_total_votes"),
        F.count("*").alias("year_n_movies"),
    )

    by_genre = (
        movies
        .select(
            "tconst", "startYear", "averageRating", "numVotes",
            F.explode(genre_array(movies)).alias("genre"),
        )
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .groupBy("startYear", "genre")
        .agg(
            F.sum("numVotes").alias("total_votes"),
            F.count("*").alias("n_movies"),
            F.count("averageRating").alias("n_rated"),
            # Vote-weighted mean rating of the genre's rated movies.
            (
                F.sum(F.col("averageRating") * F.col("numVotes"))
                / F.sum(F.when(F.col("averageRating").isNotNull(), F.col("numVotes")))
            ).alias("weighted_avg_rating"),
            F.avg("averageRating").alias("avg_rating"),
        )
        .join(year_totals, on="startYear")
        .withColumn(
            "vote_share_pct",
            F.round(F.col("total_votes") / F.col("year_total_votes") * 100, 2),
        )
    )

    window = Window.partitionBy("startYear").orderBy(
        F.col("total_votes").desc(), F.col("genre")
    )
    ranked = (
        by_genre
        .withColumn("rank", F.row_number().over(window))
        .withColumn("n_genres_in_year", F.count("*").over(Window.partitionBy("startYear")))
        .select(
            F.col("startYear").alias("year"), "rank", "genre",
            "total_votes", "vote_share_pct", "n_movies", "n_rated",
            F.round("avg_rating", 3).alias("avg_rating"),
            F.round("weighted_avg_rating", 3).alias("weighted_avg_rating"),
            "year_total_votes", "year_n_movies", "n_genres_in_year",
        )
        .orderBy("year", "rank")
        .cache()
    )

    top3 = ranked.filter(F.col("rank") <= TOP_N)

    os.makedirs(REPORTS_DIR, exist_ok=True)
    all_path = os.path.join(REPORTS_DIR, "q2_genre_popularity_by_year.csv")
    top_path = os.path.join(REPORTS_DIR, "q2_top3_genres_by_year.csv")
    ranked.toPandas().to_csv(all_path, index=False)
    top3.toPandas().to_csv(top_path, index=False)

    n_years = ranked.select("year").distinct().count()
    print(f"Movies used: {movies.count()}")
    print(f"Years: {n_years}, (year, genre) rows: {ranked.count()}, top-3 rows: {top3.count()}")

    print("\nHow often each genre is in the top 3 / ranked #1:")
    (
        top3.groupBy("genre")
        .agg(
            F.count("*").alias("years_in_top3"),
            F.sum(F.when(F.col("rank") == 1, 1).otherwise(0)).alias("years_rank1"),
            F.min("year").alias("first_year"),
            F.max("year").alias("last_year"),
        )
        .orderBy(F.col("years_in_top3").desc(), "genre")
        .show(30, truncate=False)
    )

    print("Top 3 per year (compact):")
    (
        top3.groupBy("year")
        .agg(F.sort_array(F.collect_list(F.struct("rank", "genre"))).alias("s"))
        .withColumn("top3", F.concat_ws(" > ", F.col("s.genre")))
        .drop("s")
        .orderBy("year")
        .show(50, truncate=False)
    )

    print(f"Written: {all_path}")
    print(f"Written: {top_path}")
    spark.stop()


if __name__ == "__main__":
    main()
