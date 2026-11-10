"""Q8: multi-talented film professionals and how they perform when holding
several roles on the same movie.

Definitions
-----------
- Movie: titleType == 'movie', isAdult == 0, runtimeMinutes >= 40 (all years),
  rated, numVotes >= --min-votes (default 100, same as Q4).
- Roles: director and writer from title_crew; actor from title_principals
  (category 'actor' or 'actress').
- Multi-talented: at least MIN_ROLE_MOVIES movies in each of at least two roles.
- Multi-role movie: a movie where the person holds two or more roles.

Analyses (multi-talented people only)
-------------------------------------
1. Pooled by role combination on a movie (e.g. director+writer vs director only).
2. Within-person: for each person and each multi-role combination, the mean rating
   of those movies minus the mean rating of the same person's single-role movies.

Outputs
-------
- reports/q8_multitalented_people.csv
- reports/q8_role_combo_performance.csv
- reports/q8_within_person_comparison.csv
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
MIN_ROLE_MOVIES = 2
PERCENTILE_ACCURACY = 10000
ACTING_CATEGORIES = ("actor", "actress")


def as_array(df, column):
    """Return a list column as an array, whether stored as array or CSV string."""
    if isinstance(df.schema[column].dataType, T.ArrayType):
        return F.col(column)
    return F.split(F.col(column), ",")


def exploded_ids(df, column, role):
    return (
        df.select("tconst", F.explode(as_array(df, column)).alias("nconst"))
        .withColumn("nconst", F.trim(F.col("nconst")))
        .filter(F.col("nconst").isNotNull() & (F.col("nconst") != ""))
        .withColumn("role", F.lit(role))
    )


def load_movies(spark, min_votes):
    basics = spark.read.parquet(os.path.join(CLEAN_DIR, "title_basics"))
    ratings = spark.read.parquet(os.path.join(CLEAN_DIR, "title_ratings"))
    return (
        basics
        .withColumn("runtimeMinutes", F.col("runtimeMinutes").cast("int"))
        .filter(F.col("titleType") == "movie")
        .filter(F.col("isAdult").cast("int") == 0)
        .filter(F.col("runtimeMinutes") >= MIN_RUNTIME)
        .select("tconst")
        .join(
            ratings.select(
                "tconst",
                F.col("averageRating").cast("double").alias("averageRating"),
                F.col("numVotes").cast("long").alias("numVotes"),
            ),
            on="tconst",
        )
        .filter(F.col("averageRating").isNotNull())
        .filter(F.col("numVotes") >= min_votes)
    )


def load_credits(spark):
    crew = spark.read.parquet(os.path.join(CLEAN_DIR, "title_crew"))
    principals = spark.read.parquet(os.path.join(CLEAN_DIR, "title_principals"))
    acting = (
        principals
        .filter(F.col("category").isin(*ACTING_CATEGORIES))
        .select("tconst", "nconst")
        .withColumn("role", F.lit("actor"))
    )
    return (
        exploded_ids(crew, "directors", "director")
        .unionByName(exploded_ids(crew, "writers", "writer"))
        .unionByName(acting)
        .distinct()
    )


def rating_metrics():
    return [
        F.count("*").alias("n_credits"),
        F.countDistinct("tconst").alias("n_movies"),
        F.round(F.avg("averageRating"), 3).alias("mean_rating"),
        F.round(
            F.sum(F.col("averageRating") * F.col("numVotes")) / F.sum("numVotes"), 3
        ).alias("weighted_rating"),
        F.percentile_approx("numVotes", 0.5, PERCENTILE_ACCURACY).alias("median_votes"),
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-votes", type=int, default=100)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("explore_q8").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    movies = load_movies(spark, args.min_votes)
    credits = load_credits(spark)

    # One row per (person, movie) with the set of roles held on that movie.
    person_movie = (
        credits.join(movies, on="tconst")
        .groupBy("nconst", "tconst", "averageRating", "numVotes")
        .agg(F.sort_array(F.collect_set("role")).alias("roles"))
        .withColumn("n_roles", F.size("roles"))
        .withColumn("combo", F.concat_ws("+", F.col("roles")))
        .cache()
    )

    def role_count(role):
        return F.sum(F.when(F.array_contains("roles", role), 1).otherwise(0))

    people = (
        person_movie.groupBy("nconst")
        .agg(
            F.count("*").alias("n_movies"),
            role_count("actor").alias("n_actor"),
            role_count("director").alias("n_director"),
            role_count("writer").alias("n_writer"),
            F.sum(F.when(F.col("n_roles") >= 2, 1).otherwise(0)).alias("n_multi_role_movies"),
            F.avg(F.when(F.col("n_roles") >= 2, F.col("averageRating"))).alias("mean_rating_multi"),
            F.avg(F.when(F.col("n_roles") == 1, F.col("averageRating"))).alias("mean_rating_single"),
            F.sum("numVotes").alias("total_votes"),
        )
        .withColumn(
            "talents",
            F.filter(
                F.array(
                    F.when(F.col("n_actor") >= MIN_ROLE_MOVIES, F.lit("actor")),
                    F.when(F.col("n_director") >= MIN_ROLE_MOVIES, F.lit("director")),
                    F.when(F.col("n_writer") >= MIN_ROLE_MOVIES, F.lit("writer")),
                ),
                lambda x: x.isNotNull(),
            ),
        )
        .filter(F.size("talents") >= 2)
        .withColumn("talent_combo", F.concat_ws("+", F.col("talents")))
        .withColumn(
            "rating_diff_multi_minus_single",
            F.round(F.col("mean_rating_multi") - F.col("mean_rating_single"), 3),
        )
        .drop("talents")
        .cache()
    )

    names = spark.read.parquet(os.path.join(CLEAN_DIR, "name_basics")).select(
        "nconst", "primaryName"
    )
    people_out = (
        people.join(names, on="nconst", how="left")
        .select(
            "nconst", "primaryName", "talent_combo", "n_movies",
            "n_actor", "n_director", "n_writer", "n_multi_role_movies",
            F.round("mean_rating_multi", 3).alias("mean_rating_multi"),
            F.round("mean_rating_single", 3).alias("mean_rating_single"),
            "rating_diff_multi_minus_single", "total_votes",
        )
        .orderBy(F.col("total_votes").desc(), "nconst")
        .cache()
    )

    # 1. Pooled by role combination (multi-talented people only).
    mt_movies = person_movie.join(people.select("nconst"), on="nconst")
    combo_perf = (
        mt_movies.groupBy("combo", "n_roles")
        .agg(*rating_metrics())
        .orderBy("n_roles", "combo")
    )

    # 2. Within-person comparison.
    single = (
        mt_movies.filter(F.col("n_roles") == 1)
        .groupBy("nconst")
        .agg(F.avg("averageRating").alias("single_mean"))
    )
    multi_by_combo = (
        mt_movies.filter(F.col("n_roles") >= 2)
        .groupBy("nconst", "combo")
        .agg(F.avg("averageRating").alias("multi_mean"))
    )
    multi_any = (
        mt_movies.filter(F.col("n_roles") >= 2)
        .groupBy("nconst")
        .agg(F.avg("averageRating").alias("multi_mean"))
        .withColumn("combo", F.lit("ANY multi-role"))
    )
    diffs = (
        multi_by_combo.unionByName(multi_any)
        .join(single, on="nconst")
        .withColumn("diff", F.col("multi_mean") - F.col("single_mean"))
    )
    within = (
        diffs.groupBy("combo")
        .agg(
            F.count("*").alias("n_people"),
            F.round(F.avg("diff"), 3).alias("mean_diff"),
            F.round(F.percentile_approx("diff", 0.5, PERCENTILE_ACCURACY), 3).alias("median_diff"),
            F.round(F.avg(F.when(F.col("diff") > 0, 1).otherwise(0)) * 100, 1).alias("pct_better"),
            F.round(F.avg(F.when(F.col("diff") < 0, 1).otherwise(0)) * 100, 1).alias("pct_worse"),
        )
        .orderBy("combo")
    )

    os.makedirs(REPORTS_DIR, exist_ok=True)
    paths = {
        "people": os.path.join(REPORTS_DIR, "q8_multitalented_people.csv"),
        "combo": os.path.join(REPORTS_DIR, "q8_role_combo_performance.csv"),
        "within": os.path.join(REPORTS_DIR, "q8_within_person_comparison.csv"),
    }
    people_out.toPandas().to_csv(paths["people"], index=False)
    combo_perf.toPandas().to_csv(paths["combo"], index=False)
    within.toPandas().to_csv(paths["within"], index=False)

    print(f"--min-votes {args.min_votes}, MIN_ROLE_MOVIES {MIN_ROLE_MOVIES}")
    print(f"Multi-talented people: {people_out.count()}")
    print("\nBy talent combination:")
    people.groupBy("talent_combo").count().orderBy(F.col("count").desc()).show(truncate=False)

    print("1. Pooled performance by role combination on a movie:")
    combo_perf.show(truncate=False)

    print("2. Within-person: multi-role mean rating minus single-role mean rating:")
    within.show(truncate=False)

    print("Top 15 multi-talented people by total votes:")
    people_out.select(
        "primaryName", "talent_combo", "n_movies", "n_multi_role_movies",
        "mean_rating_multi", "mean_rating_single", "rating_diff_multi_minus_single",
    ).show(15, truncate=False)

    for path in paths.values():
        print(f"Written: {path}")
    spark.stop()


if __name__ == "__main__":
    main()
