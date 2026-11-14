"""Find credited actors whose observed rating peak was early in a long career."""

import argparse

from pyspark.sql import SparkSession, functions as F

if __package__:
    from .exploration_helpers import add_input_arguments, actor_movies, inputs, with_names
else:
    from exploration_helpers import add_input_arguments, actor_movies, inputs, with_names


def analyse(credits, names, min_years=20, min_movies=15,
            early_fraction=1 / 3, min_votes=1000, min_rated=3):
    credits = credits.filter(F.col("year").isNotNull()).dropDuplicates(["nconst", "tconst"])
    careers = credits.groupBy("nconst").agg(
        F.min("year").alias("first_year"), F.max("year").alias("last_year"),
        F.count("*").alias("movie_count"),
    ).withColumn("career_years", F.col("last_year") - F.col("first_year"))
    careers = careers.filter(
        (F.col("career_years") >= min_years) & (F.col("movie_count") >= min_movies)
    ).withColumn(
        "early_end_year",
        F.ceil(F.col("first_year") + early_fraction * F.col("career_years")).cast("int") - 1,
    )
    observations = credits.join(careers, "nconst").withColumn(
        "reliable_rating",
        F.when((F.col("votes") >= min_votes) & F.col("rating").between(0, 10), F.col("rating")),
    )
    early = F.col("year") <= F.col("early_end_year")
    result = observations.groupBy(
        "nconst", "first_year", "last_year", "career_years", "movie_count", "early_end_year"
    ).agg(
        F.count(F.when(early, F.col("reliable_rating"))).alias("early_rated_movies"),
        F.count(F.when(~early, F.col("reliable_rating"))).alias("later_rated_movies"),
        F.avg(F.when(early, F.col("reliable_rating"))).alias("early_mean_rating"),
        F.avg(F.when(~early, F.col("reliable_rating"))).alias("later_mean_rating"),
        F.max(F.when(early, F.col("reliable_rating"))).alias("early_best_rating"),
        F.max(F.when(~early, F.col("reliable_rating"))).alias("later_best_rating"),
    ).filter(
        (F.col("early_rated_movies") >= min_rated)
        & (F.col("later_rated_movies") >= min_rated)
        & (F.col("early_mean_rating") > F.col("later_mean_rating"))
        & (F.col("early_best_rating") > F.col("later_best_rating"))
    ).withColumn("rating_gap", F.col("early_mean_rating") - F.col("later_mean_rating"))
    return with_names(result, names).orderBy(F.desc("rating_gap"), "nconst")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_input_arguments(parser)
    parser.add_argument("--output", required=True, help="New CSV output directory")
    parser.add_argument("--min-career-years", type=int, default=20)
    parser.add_argument("--min-movies", type=int, default=15)
    parser.add_argument("--early-fraction", type=float, default=1 / 3)
    parser.add_argument("--min-votes", type=int, default=1000)
    parser.add_argument("--min-rated-per-period", type=int, default=3)
    args = parser.parse_args()
    if not 0 < args.early_fraction < 1:
        parser.error("--early-fraction must be between 0 and 1")
    if min(args.min_career_years, args.min_movies, args.min_rated_per_period) < 1 or args.min_votes < 0:
        parser.error("Counts and duration must be positive; --min-votes may be zero")
    spark = SparkSession.builder.appName("IMDb Exploration Q7").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = inputs(spark, args)
        result = analyse(
            actor_movies(tables), tables["names"], args.min_career_years,
            args.min_movies, args.early_fraction, args.min_votes, args.min_rated_per_period,
        ).cache()
        print(f"Q7 matching actors: {result.count()}", flush=True)
        print("Ratings are a reception proxy; career coverage is limited to principal credits.")
        result.show(20, truncate=False)
        result.coalesce(1).write.csv(args.output, header=True)
        result.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
