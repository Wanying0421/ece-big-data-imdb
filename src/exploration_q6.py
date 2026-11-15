"""Find actors successful across original film languages supplied by Wikidata."""

import argparse

from pyspark.sql import SparkSession, functions as F

if __package__:
    from .exploration_helpers import add_input_arguments, actor_movies, inputs, with_names
else:
    from exploration_helpers import add_input_arguments, actor_movies, inputs, with_names


LANGUAGE_SCHEMA = (
    "tconst STRING, note STRING, source_url STRING, languages ARRAY<STRUCT<"
    "language_id: STRING, language_name: STRING, source_url: STRING>>"
)


def successful_credits(credits, min_rating=7, min_votes=1000):
    return credits.filter(
        F.col("rating").between(min_rating, 10) & (F.col("votes") >= min_votes)
    ).dropDuplicates(["nconst", "tconst"])


def analyse(successful, names, language_records):
    # Ambiguous IMDb-to-Wikidata matches are not used to infer a film's languages.
    languages = (
        language_records.filter(F.col("note").isNull())
        .select("tconst", F.explode("languages").alias("language"))
        .select("tconst", "language.language_id", "language.language_name")
        .dropna(subset=["tconst", "language_id"])
        .dropDuplicates(["tconst", "language_id"])
    )
    observations = successful.join(languages, "tconst")
    language_counts = observations.groupBy("nconst", "language_id", "language_name").agg(
        F.countDistinct("tconst").alias("successful_movies")
    )
    summary = observations.groupBy("nconst").agg(
        F.countDistinct("language_id").alias("language_count"),
        F.countDistinct("tconst").alias("successful_movies_with_language"),
        F.concat_ws(", ", F.sort_array(F.collect_set("language_name"))).alias("languages"),
    ).filter(
        (F.col("language_count") >= 2) & (F.col("successful_movies_with_language") >= 2)
    ).withColumn("at_least_three_languages", F.col("language_count") >= 3)
    summary = with_names(summary, names).orderBy(F.desc("language_count"), "nconst")
    detail = observations.join(summary.select("nconst"), "nconst", "left_semi").select(
        "nconst", "tconst", "primaryTitle", "rating", "votes", "language_id", "language_name"
    ).dropDuplicates()
    return summary, language_counts.join(summary.select("nconst"), "nconst", "left_semi"), detail


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_input_arguments(parser)
    parser.add_argument("--min-rating", type=float, default=7)
    parser.add_argument("--min-votes", type=int, default=1000)
    parser.add_argument("--candidates-output", help="New CSV directory for language collection")
    parser.add_argument("--languages", help="JSONL produced by collect_film_languages.py")
    parser.add_argument("--output", help="New directory containing Q6 CSV results")
    args = parser.parse_args()
    if not 0 <= args.min_rating <= 10 or args.min_votes < 0:
        parser.error("Invalid rating or vote threshold")
    if not args.candidates_output and not args.languages:
        parser.error("Provide --candidates-output or --languages")
    if args.languages and not args.output:
        parser.error("--output is required with --languages")
    spark = SparkSession.builder.appName("IMDb Exploration Q6").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        tables = inputs(spark, args)
        successful = successful_credits(actor_movies(tables), args.min_rating, args.min_votes).cache()
        potential_actors = successful.groupBy("nconst").count().filter(F.col("count") >= 2)
        successful = successful.join(potential_actors.select("nconst"), "nconst", "left_semi")
        candidates = successful.select("tconst", "primaryTitle", "rating", "votes").dropDuplicates(["tconst"])
        if args.candidates_output:
            candidates.orderBy("tconst").coalesce(1).write.csv(args.candidates_output, header=True)
        if args.languages:
            records = spark.read.schema(LANGUAGE_SCHEMA).json(args.languages)
            covered = records.filter(F.col("note").isNull() & (F.size("languages") > 0))
            covered_ids = covered.select("tconst").distinct()
            print(f"Q6 candidate films: {candidates.count()}", flush=True)
            print(f"Candidate films with known original languages: {candidates.join(covered_ids, 'tconst', 'left_semi').count()}", flush=True)
            summary, by_language, detail = analyse(successful, tables["names"], records)
            summary = summary.cache()
            print(f"Q6 actors with at least two languages: {summary.count()}", flush=True)
            print(f"Q6 actors with at least three languages: {summary.filter('at_least_three_languages').count()}", flush=True)
            summary.show(20, truncate=False)
            for frame, name in ((summary, "actors"), (by_language, "actor_languages"), (detail, "evidence")):
                frame.coalesce(1).write.csv(f"{args.output}/{name}", header=True)
            summary.filter("at_least_three_languages").coalesce(1).write.csv(
                f"{args.output}/actors_three_languages", header=True)
            summary.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
