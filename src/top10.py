"""Build the list of the top N feature films per genre and per year.

Feature film : titleType 'movie', not adult, runtime >= MIN_RUNTIME minutes.
Ranking      : popularity (numVotes), ties broken by rating, then by id.
Genres       : a film with several genres takes part in the ranking of each of them.

Input : data/parquet_clean (output of prep_clean.py)
Output: reports/top10_by_genre_year.csv  one row per (year, genre, rank)
        reports/top10_films.csv          one row per distinct film (input of the enrichment)
Both files are small, so they are committed to Git.
"""

import csv

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

PARQUET_DIR = "/home/jovyan/work/data/parquet_clean"
OUT_DIR = "/home/jovyan/work/reports"

START_YEAR = 1980
END_YEAR = 2025
TOP_N = 10
MIN_RUNTIME = 40  # minutes, common definition of a feature-length film


def write_csv(df, path):
    rows = df.collect()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(df.columns)
        writer.writerows([[r[c] for c in df.columns] for r in rows])
    print(f"wrote {len(rows)} rows to {path}")


def main():
    spark = SparkSession.builder.appName("top10").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tb = spark.read.parquet(f"{PARQUET_DIR}/title_basics")
    tr = spark.read.parquet(f"{PARQUET_DIR}/title_ratings")

    films = (
        tb.where(
            (F.col("titleType") == "movie")
            & (F.col("isAdult") == 0)
            & F.col("startYear").between(START_YEAR, END_YEAR)
            & (F.col("runtimeMinutes") >= MIN_RUNTIME)
            & F.col("genres").isNotNull()
        )
        .join(tr, "tconst")
        .select(
            "tconst",
            "primaryTitle",
            "startYear",
            "runtimeMinutes",
            "averageRating",
            "numVotes",
            F.explode(F.split("genres", ",")).alias("genre"),
        )
    )

    ranking = Window.partitionBy("startYear", "genre").orderBy(
        F.col("numVotes").desc(), F.col("averageRating").desc(), F.col("tconst")
    )
    top = (
        films.withColumn("rank", F.row_number().over(ranking))
        .where(F.col("rank") <= TOP_N)
        .withColumnRenamed("startYear", "year")
        .select(
            "year", "genre", "rank", "tconst", "primaryTitle",
            "runtimeMinutes", "averageRating", "numVotes",
        )
        .orderBy("year", "genre", "rank")
    )
    top.cache()

    distinct_films = (
        top.groupBy("tconst", "primaryTitle", "year", "runtimeMinutes", "averageRating", "numVotes")
        .agg(F.concat_ws(",", F.sort_array(F.collect_set("genre"))).alias("top10_genres"))
        .orderBy("year", "tconst")
    )

    write_csv(top, f"{OUT_DIR}/top10_by_genre_year.csv")
    write_csv(distinct_films, f"{OUT_DIR}/top10_films.csv")

    groups = top.groupBy("year", "genre").count()
    print("\n=== TOP 10 SUMMARY ===")
    print(f"years: {START_YEAR}-{END_YEAR}, top {TOP_N}, min runtime {MIN_RUNTIME} min")
    print(f"(year, genre) groups: {groups.count()}")
    print(f"groups with fewer than {TOP_N} films: {groups.where(F.col('count') < TOP_N).count()}")
    print(f"rows in top10_by_genre_year: {top.count()}")
    print(f"distinct films to enrich: {distinct_films.count()}")
    print(f"distinct genres: {top.select('genre').distinct().count()}")
    spark.stop()


if __name__ == "__main__":
    main()
