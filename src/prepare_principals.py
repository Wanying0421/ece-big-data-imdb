import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    parser = argparse.ArgumentParser(description="Select IMDb principals for enrichment.")
    parser.add_argument("--films", required=True, help="Top films Parquet or CSV path with a tconst column")
    parser.add_argument("--principals", required=True, help="IMDb principals Parquet path")
    parser.add_argument("--output", required=True, help="Output CSV directory")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Prepare IMDb principals").getOrCreate()

    if args.films.lower().endswith(".csv"):
        films = spark.read.csv(args.films, header=True, multiLine=True, escape='"')
    else:
        films = spark.read.parquet(args.films)
    films = films.select("tconst").distinct()
    credits = spark.read.parquet(args.principals).select(
        "tconst", "nconst", F.col("ordering").cast("int").alias("ordering")
    )

    unique_credits = (
        credits.join(films, on="tconst", how="left_semi")
        .filter(F.col("nconst").rlike(r"^nm[0-9]+$") & F.col("ordering").isNotNull())
        .groupBy("tconst", "nconst")
        .agg(F.min("ordering").alias("ordering"))
    )

    credit_order = Window.partitionBy("tconst").orderBy("ordering", "nconst")
    selected = (
        unique_credits.withColumn("rank", F.row_number().over(credit_order))
        .filter(F.col("rank") <= 3)
        .select("tconst", "nconst")
    )

    selected.coalesce(1).write.csv(args.output, header=True)
    spark.stop()


if __name__ == "__main__":
    main()