import argparse

from pyspark.sql import SparkSession, Window, functions as F


def read_imdb(spark, path):
    if path.endswith((".tsv", ".tsv.gz")):
        return spark.read.csv(
            path, header=True, sep="\t", nullValue=r"\N", quote=""
        )
    return spark.read.parquet(path)


def main():
    parser = argparse.ArgumentParser(description="Run IMDb exploration Q5.")
    parser.add_argument("--episodes", required=True)
    parser.add_argument("--ratings", required=True)
    parser.add_argument("--titles", required=True)
    parser.add_argument("--output", required=True, help="New CSV output directory")
    parser.add_argument("--min-rated-episodes", type=int, default=10)
    args = parser.parse_args()
    if args.min_rated_episodes < 2:
        parser.error("--min-rated-episodes must be at least 2")

    spark = SparkSession.builder.appName("IMDb Exploration Q5").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_episodes = read_imdb(spark, args.episodes).select(
            F.col("tconst").alias("episode_id"),
            F.col("parentTconst").alias("series_id"),
            F.col("seasonNumber").cast("int").alias("season"),
        )
        valid_episode = (
            F.col("episode_id").isNotNull()
            & F.col("series_id").isNotNull()
            & F.col("season").isNotNull()
        )
        coverage = raw_episodes.agg(
            F.count("*").alias("total"),
            F.count(F.when(F.col("season").isNull(), 1)).alias("missing_season"),
            F.count(F.when(~valid_episode, 1)).alias("dropped"),
        ).first()
        missing_pct = 100 * coverage.missing_season / max(coverage.total, 1)
        print(
            f"Episode rows: {coverage.total:,}; "
            f"missing season: {coverage.missing_season:,} ({missing_pct:.2f}%); "
            f"total dropped: {coverage.dropped:,}",
            flush=True,
        )

        ratings = read_imdb(spark, args.ratings).select(
            F.col("tconst").alias("episode_id"),
            F.col("averageRating").cast("decimal(3,1)").alias("rating"),
        )
        titles = read_imdb(spark, args.titles).filter(
            F.col("titleType") == "tvSeries"
        ).select(
            F.col("tconst").alias("series_id"),
            F.col("primaryTitle").alias("series_title"),
        )

        # Keep unrated episodes when counting episodes and ordering seasons.
        seasons = (
            raw_episodes.filter(valid_episode)
            .join(ratings, "episode_id", "left")
            .groupBy("series_id", "season")
            .agg(
                F.countDistinct("episode_id").alias("episode_count"),
                F.count("rating").alias("rated_count"),
                F.max("rating").alias("season_best"),
                F.min("rating").alias("season_worst"),
            )
        )

        series_window = Window.partitionBy("series_id")
        season_order = series_window.orderBy("season")
        seasons = (
            seasons.withColumn("season_rank", F.dense_rank().over(season_order))
            .withColumn("last_season", F.max("season").over(series_window))
            .withColumn("best_rating", F.max("season_best").over(series_window))
            .withColumn("worst_rating", F.min("season_worst").over(series_window))
        )

        has_best = F.col("season_best") == F.col("best_rating")
        has_worst = F.col("season_worst") == F.col("worst_rating")
        first_two = F.col("season_rank") <= 2
        is_last = F.col("season") == F.col("last_season")

        # A tied extreme counts if it occurs in at least one required season.
        # The strict rule also checks whether it occurs outside those seasons.
        matches = (
            seasons.groupBy("series_id", "last_season", "best_rating", "worst_rating")
            .agg(
                F.count(F.when(F.col("episode_count") >= 10, 1)).alias(
                    "qualifying_seasons"
                ),
                F.sum("rated_count").alias("rated_episodes"),
                F.count(F.when(has_best & first_two, 1)).alias("best_inside"),
                F.count(F.when(has_worst & is_last, 1)).alias("worst_inside"),
                F.count(F.when(has_best & ~first_two, 1)).alias("best_outside"),
                F.count(F.when(has_worst & ~is_last, 1)).alias("worst_outside"),
            )
            .filter(
                (F.col("qualifying_seasons") >= 3)
                & (F.col("rated_episodes") >= args.min_rated_episodes)
                & (F.col("best_rating") > F.col("worst_rating"))
                & (F.col("best_inside") > 0)
                & (F.col("worst_inside") > 0)
            )
            .withColumn(
                "strict_match",
                (F.col("best_outside") == 0) & (F.col("worst_outside") == 0),
            )
            .join(titles, "series_id")
            .withColumn("rating_gap", F.col("best_rating") - F.col("worst_rating"))
            .select(
                "series_id", "series_title", "qualifying_seasons", "last_season",
                "rated_episodes", "best_rating", "worst_rating", "rating_gap",
                "strict_match",
            )
            .persist()
        )

        totals = matches.agg(
            F.count("*").alias("total"),
            F.count(F.when(F.col("strict_match"), 1)).alias("strict"),
        ).first()
        print(f"Matching series: {totals.total:,}; strict matches: {totals.strict:,}")

        ordered = matches.orderBy(F.desc("rating_gap"), "series_id")
        ordered.show(20, truncate=False)
        ordered.coalesce(1).write.csv(args.output, header=True)
        matches.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()