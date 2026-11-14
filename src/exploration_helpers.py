"""Inputs shared by exploration questions 6 and 7."""

from pyspark.sql import functions as F


def add_input_arguments(parser):
    parser.add_argument(
        "--data-dir", default="/home/jovyan/work/data/parquet_clean"
    )
    for name in ("titles", "ratings", "principals", "names"):
        parser.add_argument(f"--{name}", help="Override the corresponding input path")


def read_table(spark, path):
    if path.endswith((".tsv", ".tsv.gz")):
        return spark.read.csv(
            path, header=True, sep="\t", nullValue=r"\N", quote=""
        )
    return spark.read.parquet(path)


def inputs(spark, args):
    tables = {
        key: read_table(spark, override or f"{args.data_dir}/{table}")
        for key, override, table in (
            ("titles", args.titles, "title_basics"),
            ("ratings", args.ratings, "title_ratings"),
            ("principals", args.principals, "title_principals"),
            ("names", args.names, "name_basics"),
        )
    }
    return tables


def actor_movies(tables):
    movies = (
        tables["titles"]
        .filter((F.col("titleType") == "movie") & (F.col("isAdult").cast("int") == 0))
        .select(
            "tconst", "primaryTitle", F.col("startYear").cast("int").alias("year")
        )
        .dropDuplicates(["tconst"])
    )
    credits = (
        tables["principals"]
        .filter(F.col("category").isin("actor", "actress"))
        .select("tconst", "nconst")
        .dropna()
        .dropDuplicates()
    )
    ratings = tables["ratings"].select(
        "tconst", F.col("averageRating").cast("double").alias("rating"),
        F.col("numVotes").cast("long").alias("votes"),
    ).dropDuplicates(["tconst"])
    return credits.join(movies, "tconst").join(ratings, "tconst", "left")


def with_names(result, names):
    return result.join(
        names.select("nconst", "primaryName").dropDuplicates(["nconst"]),
        "nconst", "left",
    )
