"""Q3: compare the top three genres of each year (Q2) with the rest of the genres.

Movie selection and top-3 ranking are identical to explore_q2.py:
- titleType == 'movie', isAdult == 0, runtimeMinutes >= 40, 1980 <= startYear <= 2025
- popularity of (year, genre) = sum of numVotes; ties broken by genre name

Two comparison levels
---------------------
1. Genre level (one row per year): votes and movies per genre for the top-3 genres
   versus the average other genre, and the top-3 share of all genre-level votes
   (multi-genre movies count in each of their genres).
2. Movie level (one row per year and group): each movie is assigned to exactly one
   group, 'top3' if any of its genres is in that year's top 3, otherwise 'rest'.
   Compares number of movies, share of movies and votes, mean / median votes per
   movie and mean / vote-weighted rating. Movies without genres are excluded.

Outputs
-------
- reports/q3_genre_level_by_year.csv
- reports/q3_movie_level_by_year.csv
- reports/q3_overall.csv
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
PERCENTILE_ACCURACY = 10000


def genre_array(df):
    """Return genres as an array column, whether stored as array or CSV string."""
    if isinstance(df.schema["genres"].dataType, T.ArrayType):
        return F.col("genres")
    return F.split(F.col("genres"), ",")


def weighted_rating():
    """Vote-weighted mean rating, ignoring unrated movies."""
    return F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum(
        F.when(F.col("averageRating").isNotNull(), F.col("numVotes"))
    )


def movie_metrics():
    """Aggregations shared by the per-year and overall movie-level tables."""
    return [
        F.count("*").alias("n_movies"),
        F.sum("numVotes").alias("total_votes"),
        F.round(F.avg("numVotes"), 1).alias("mean_votes"),
        F.percentile_approx("numVotes", 0.5, PERCENTILE_ACCURACY).alias("median_votes"),
        F.count("averageRating").alias("n_rated"),
        F.round(F.avg("averageRating"), 3).alias("mean_rating"),
        F.round(weighted_rating(), 3).alias("weighted_rating"),
    ]


def load_movies(spark):
    basics = spark.read.parquet(os.path.join(CLEAN_DIR, "title_basics"))
    ratings = spark.read.parquet(os.path.join(CLEAN_DIR, "title_ratings"))
    return (
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
    )


def main():
    spark = SparkSession.builder.appName("explore_q3").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    movies = load_movies(spark)
    exploded = (
        movies
        .select(
            "tconst", "startYear", "averageRating", "numVotes",
            F.explode(genre_array(movies)).alias("genre"),
        )
        .withColumn("genre", F.trim(F.col("genre")))
        .filter(F.col("genre").isNotNull() & (F.col("genre") != ""))
        .cache()
    )

    # Same ranking as Q2.
    rank_window = Window.partitionBy("startYear").orderBy(
        F.col("total_votes").desc(), F.col("genre")
    )
    ranked = (
        exploded.groupBy("startYear", "genre")
        .agg(F.sum("numVotes").alias("total_votes"), F.count("*").alias("n_movies"))
        .withColumn("rank", F.row_number().over(rank_window))
        .withColumn("is_top3", F.col("rank") <= TOP_N)
        .cache()
    )

    # ---- 1. Genre level -------------------------------------------------------
    top = F.col("is_top3")
    genre_level = (
        ranked.groupBy("startYear")
        .agg(
            F.sort_array(
                F.collect_list(F.when(top, F.struct("rank", "genre")))
            ).alias("s"),
            F.sum(F.when(top, 1).otherwise(0)).alias("n_top3_genres"),
            F.sum(F.when(~top, 1).otherwise(0)).alias("n_rest_genres"),
            F.sum(F.when(top, F.col("total_votes")).otherwise(0)).alias("top3_votes"),
            F.sum(F.when(~top, F.col("total_votes")).otherwise(0)).alias("rest_votes"),
            F.sum(F.when(top, F.col("n_movies")).otherwise(0)).alias("top3_movies"),
            F.sum(F.when(~top, F.col("n_movies")).otherwise(0)).alias("rest_movies"),
        )
        .withColumn("top3_genres", F.concat_ws(", ", F.col("s.genre")))
        .withColumn(
            "top3_vote_share_pct",
            F.round(
                F.col("top3_votes") / (F.col("top3_votes") + F.col("rest_votes")) * 100, 2
            ),
        )
        .withColumn(
            "votes_per_top3_genre",
            F.round(F.col("top3_votes") / F.col("n_top3_genres"), 1),
        )
        .withColumn(
            "votes_per_rest_genre",
            F.round(F.col("rest_votes") / F.col("n_rest_genres"), 1),
        )
        .withColumn(
            "votes_ratio_top3_vs_rest",
            F.round(F.col("votes_per_top3_genre") / F.col("votes_per_rest_genre"), 2),
        )
        .withColumn(
            "movies_per_top3_genre",
            F.round(F.col("top3_movies") / F.col("n_top3_genres"), 1),
        )
        .withColumn(
            "movies_per_rest_genre",
            F.round(F.col("rest_movies") / F.col("n_rest_genres"), 1),
        )
        .select(
            F.col("startYear").alias("year"), "top3_genres",
            "n_top3_genres", "n_rest_genres",
            "top3_votes", "rest_votes", "top3_vote_share_pct",
            "votes_per_top3_genre", "votes_per_rest_genre", "votes_ratio_top3_vs_rest",
            "movies_per_top3_genre", "movies_per_rest_genre",
        )
        .orderBy("year")
    )

    # ---- 2. Movie level -------------------------------------------------------
    movie_group = (
        exploded
        .join(ranked.select("startYear", "genre", "is_top3"), on=["startYear", "genre"])
        .groupBy("tconst", "startYear", "averageRating", "numVotes")
        .agg(F.max(F.col("is_top3").cast("int")).alias("in_top3"))
        .withColumn("group", F.when(F.col("in_top3") == 1, "top3").otherwise("rest"))
        .cache()
    )

    year_window = Window.partitionBy("startYear")
    movie_level = (
        movie_group.groupBy("startYear", "group")
        .agg(*movie_metrics())
        .withColumn(
            "movie_share_pct",
            F.round(F.col("n_movies") / F.sum("n_movies").over(year_window) * 100, 2),
        )
        .withColumn(
            "vote_share_pct",
            F.round(F.col("total_votes") / F.sum("total_votes").over(year_window) * 100, 2),
        )
        .withColumnRenamed("startYear", "year")
        .select(
            "year", "group", "n_movies", "movie_share_pct",
            "total_votes", "vote_share_pct", "mean_votes", "median_votes",
            "n_rated", "mean_rating", "weighted_rating",
        )
        .orderBy("year", F.col("group").desc())
        .cache()
    )

    # ---- Overall (all years pooled + mean of yearly shares) -------------------
    pooled = movie_group.groupBy("group").agg(*movie_metrics())
    mean_shares = movie_level.groupBy("group").agg(
        F.round(F.avg("movie_share_pct"), 2).alias("avg_yearly_movie_share_pct"),
        F.round(F.avg("vote_share_pct"), 2).alias("avg_yearly_vote_share_pct"),
    )
    overall = pooled.join(mean_shares, on="group").orderBy(F.col("group").desc())

    # ---- Write ----------------------------------------------------------------
    os.makedirs(REPORTS_DIR, exist_ok=True)
    paths = {
        "genre": os.path.join(REPORTS_DIR, "q3_genre_level_by_year.csv"),
        "movie": os.path.join(REPORTS_DIR, "q3_movie_level_by_year.csv"),
        "overall": os.path.join(REPORTS_DIR, "q3_overall.csv"),
    }
    genre_level.toPandas().to_csv(paths["genre"], index=False)
    movie_level.toPandas().to_csv(paths["movie"], index=False)
    overall.toPandas().to_csv(paths["overall"], index=False)

    # ---- Console summary ------------------------------------------------------
    print(f"Movies with at least one genre: {movie_group.count()}")

    print("\nOverall, movie level (1980-2025):")
    overall.show(truncate=False)

    print("Genre level, averaged by decade:")
    (
        genre_level
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade")
        .agg(
            F.round(F.avg("top3_vote_share_pct"), 2).alias("top3_vote_share_pct"),
            F.round(F.avg("votes_ratio_top3_vs_rest"), 2).alias("votes_ratio"),
            F.round(F.avg("movies_per_top3_genre"), 1).alias("movies_per_top3_genre"),
            F.round(F.avg("movies_per_rest_genre"), 1).alias("movies_per_rest_genre"),
        )
        .orderBy("decade")
        .show(truncate=False)
    )

    print("Movie level, averaged by decade:")
    (
        movie_level
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("decade", "group")
        .agg(
            F.round(F.avg("movie_share_pct"), 2).alias("movie_share_pct"),
            F.round(F.avg("vote_share_pct"), 2).alias("vote_share_pct"),
            F.round(F.avg("mean_votes"), 1).alias("mean_votes"),
            F.round(F.avg("median_votes"), 1).alias("median_votes"),
            F.round(F.avg("mean_rating"), 3).alias("mean_rating"),
            F.round(F.avg("weighted_rating"), 3).alias("weighted_rating"),
        )
        .orderBy("decade", F.col("group").desc())
        .show(truncate=False)
    )

    for path in paths.values():
        print(f"Written: {path}")
    spark.stop()


if __name__ == "__main__":
    main()
