"""Q4: directors with exactly one movie rated >= 8.0 and multiple other movies
whose average rating is <= 6.0.

Definitions
-----------
- Movie: titleType == 'movie', isAdult == 0, runtimeMinutes >= 40 (all years),
  rated, and numVotes >= --min-votes (default 0 = no filter).
- Directors come from title_crew.directors (cleaned).
- Qualifying director:
    * exactly one movie with averageRating >= HIGH_RATING
    * at least MIN_OTHER other movies
    * simple mean rating of those other movies <= LOW_AVG
- n_other_le6 is also reported, for the stricter reading
  "every other movie is rated <= 6.0".

Outputs
-------
- reports/q4_directors.csv              : qualifying directors for the chosen --min-votes
- reports/q4_threshold_sensitivity.csv  : number of directors per vote threshold

Usage
-----
spark-submit explore_q4.py --min-votes 100
"""

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

BASE_DIR = "/home/jovyan/work"
CLEAN_DIR = os.path.join(BASE_DIR, "data", "parquet_clean")
REPORTS_DIR = os.path.join(BASE_DIR, "reports")

MIN_RUNTIME = 40
HIGH_RATING = 8.0
LOW_AVG = 6.0
MIN_OTHER = 2
VOTE_THRESHOLDS = (0, 10, 100, 1000)


def as_array(df, column):
    """Return a list column as an array, whether stored as array or CSV string."""
    if isinstance(df.schema[column].dataType, T.ArrayType):
        return F.col(column)
    return F.split(F.col(column), ",")


def load_director_movies(spark):
    """One row per (director, rated feature-length movie)."""
    basics = spark.read.parquet(os.path.join(CLEAN_DIR, "title_basics"))
    ratings = spark.read.parquet(os.path.join(CLEAN_DIR, "title_ratings"))
    crew = spark.read.parquet(os.path.join(CLEAN_DIR, "title_crew"))

    movies = (
        basics
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
        .filter(F.col("titleType") == "movie")
        .filter(F.col("isAdult").cast("int") == 0)
        .filter(F.col("runtimeMinutes") >= MIN_RUNTIME)
        .select("tconst", "primaryTitle", F.col("startYear").cast("int").alias("startYear"))
        .join(
            ratings.select(
                "tconst",
                F.col("averageRating").cast("double").alias("averageRating"),
                F.col("numVotes").cast("long").alias("numVotes"),
            ),
            on="tconst",
        )
        .filter(F.col("averageRating").isNotNull())
    )

    directors = (
        crew
        .select("tconst", F.explode(as_array(crew, "directors")).alias("nconst"))
        .withColumn("nconst", F.trim(F.col("nconst")))
        .filter(F.col("nconst").isNotNull() & (F.col("nconst") != ""))
        .distinct()
    )
    return directors.join(movies, on="tconst")


def qualifying_directors(director_movies, min_votes):
    """Per-director aggregates and the Q4 condition."""
    df = director_movies.filter(F.col("numVotes") >= min_votes)
    is_high = F.col("averageRating") >= HIGH_RATING
    high_struct = F.struct(
        "tconst", "primaryTitle", "startYear", "averageRating", "numVotes"
    )
    return (
        df.groupBy("nconst")
        .agg(
            F.count("*").alias("n_movies"),
            F.sum(F.when(is_high, 1).otherwise(0)).alias("n_high"),
            F.first(F.when(is_high, high_struct), ignorenulls=True).alias("high"),
            F.sum(F.when(~is_high, 1).otherwise(0)).alias("n_other"),
            F.avg(F.when(~is_high, F.col("averageRating"))).alias("avg_other_rating"),
            F.sum(F.when(F.col("averageRating") <= LOW_AVG, 1).otherwise(0))
            .alias("n_other_le6"),
            F.min(F.when(~is_high, F.col("averageRating"))).alias("min_other_rating"),
            F.max(F.when(~is_high, F.col("averageRating"))).alias("max_other_rating"),
        )
        .filter(
            (F.col("n_high") == 1)
            & (F.col("n_other") >= MIN_OTHER)
            & (F.col("avg_other_rating") <= LOW_AVG)
        )
        .withColumn("all_others_le6", F.col("n_other_le6") == F.col("n_other"))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-votes", type=int, default=0)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("explore_q4").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    director_movies = load_director_movies(spark).cache()
    print(f"(director, movie) pairs: {director_movies.count()}")
    print(f"Distinct directors     : {director_movies.select('nconst').distinct().count()}")

    # Sensitivity to the minimum number of votes.
    rows = []
    for threshold in VOTE_THRESHOLDS:
        q = qualifying_directors(director_movies, threshold).cache()
        n_avg = q.count()
        n_strict = q.filter(F.col("all_others_le6")).count()
        rows.append((threshold, n_avg, n_strict))
        q.unpersist()
    sensitivity = spark.createDataFrame(
        rows, ["min_votes", "n_directors_avg_le6", "n_directors_all_le6"]
    )

    # Detailed list for the chosen threshold.
    names = spark.read.parquet(os.path.join(CLEAN_DIR, "name_basics")).select(
        "nconst", "primaryName"
    )
    result = (
        qualifying_directors(director_movies, args.min_votes)
        .join(names, on="nconst", how="left")
        .select(
            "nconst", "primaryName", "n_movies",
            F.col("high.tconst").alias("high_tconst"),
            F.col("high.primaryTitle").alias("high_title"),
            F.col("high.startYear").alias("high_year"),
            F.col("high.averageRating").alias("high_rating"),
            F.col("high.numVotes").alias("high_votes"),
            "n_other",
            F.round("avg_other_rating", 3).alias("avg_other_rating"),
            "min_other_rating", "max_other_rating",
            "n_other_le6", "all_others_le6",
        )
        .withColumn("min_votes_filter", F.lit(args.min_votes))
        .orderBy(F.col("high_votes").desc(), "nconst")
        .cache()
    )

    os.makedirs(REPORTS_DIR, exist_ok=True)
    list_path = os.path.join(REPORTS_DIR, "q4_directors.csv")
    sens_path = os.path.join(REPORTS_DIR, "q4_threshold_sensitivity.csv")
    result.toPandas().to_csv(list_path, index=False)
    sensitivity.toPandas().to_csv(sens_path, index=False)

    print("\nSensitivity to minimum votes per movie:")
    sensitivity.show(truncate=False)

    print(f"Chosen --min-votes: {args.min_votes}, directors: {result.count()}")
    print("Top 20 by votes of their >= 8.0 movie:")
    result.select(
        "primaryName", "high_title", "high_year", "high_rating", "high_votes",
        "n_other", "avg_other_rating", "all_others_le6",
    ).show(20, truncate=40)

    print(f"Written: {list_path}")
    print(f"Written: {sens_path}")
    spark.stop()


if __name__ == "__main__":
    main()
