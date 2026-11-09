"""Detect data quality issues in the IMDb tables (detection only).

Results are printed and saved to reports/quality_issues.csv.
"""

import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PARQUET_DIR = "/home/jovyan/work/data/parquet"
OUT_FILE = "/home/jovyan/work/reports/quality_issues.csv"
CURRENT_YEAR = 2026

results = []


def read(spark, table):
    return spark.read.parquet(f"{PARQUET_DIR}/{table}")


def add(issue, table, description, affected, total):
    pct = round(100.0 * affected / total, 4) if total else 0.0
    results.append(
        {
            "issue": issue,
            "table": table,
            "description": description,
            "affected_rows": affected,
            "total_rows": total,
            "pct": pct,
        }
    )
    print(f"[{issue}] {table}: {description} -> {affected} / {total} ({pct}%)")


def condition_check(issue, table, description, df, condition):
    """Count the rows of df that satisfy a 'bad row' condition."""
    row = df.agg(
        F.count(F.lit(1)).alias("total"),
        F.sum(F.when(condition, 1).otherwise(0)).alias("bad"),
    ).collect()[0]
    add(issue, table, description, row["bad"] or 0, row["total"])


def duplicate_check(table, df, keys):
    total = df.count()
    distinct = df.dropDuplicates(keys).count()
    add("duplicate_key", table, f"duplicate rows on {keys}", total - distinct, total)


def fk_check(child_table, child_df, child_col, parent_table, parent_df, parent_col):
    """Count child keys that have no matching row in the parent table."""
    child = child_df.select(F.col(child_col).alias("k")).where(F.col("k").isNotNull())
    parent = (
        parent_df.select(F.col(parent_col).alias("k"))
        .distinct()
        .withColumn("found", F.lit(1))
    )
    row = (
        child.join(parent, "k", "left")
        .agg(
            F.count(F.lit(1)).alias("total"),
            F.sum(F.when(F.col("found").isNull(), 1).otherwise(0)).alias("orphans"),
        )
        .collect()[0]
    )
    add(
        "missing_parent",
        child_table,
        f"{child_col} not found in {parent_table}.{parent_col}",
        row["orphans"] or 0,
        row["total"],
    )


def exploded(df, col):
    """One row per id for comma separated id lists such as 'nm1,nm2'."""
    return df.select(F.explode(F.split(F.col(col), ",")).alias(col))


def main():
    spark = (
        SparkSession.builder.appName("prep_quality")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    nb = read(spark, "name_basics")
    tb = read(spark, "title_basics")
    tr = read(spark, "title_ratings")
    te = read(spark, "title_episode")
    tc = read(spark, "title_crew")
    tp = read(spark, "title_principals")
    ta = read(spark, "title_akas")

    # ---- Impossible or suspicious values ----
    condition_check(
        "death_before_birth", "name_basics", "deathYear < birthYear", nb,
        F.col("deathYear") < F.col("birthYear"),
    )
    condition_check(
        "future_year", "name_basics", f"birthYear or deathYear > {CURRENT_YEAR}", nb,
        (F.col("birthYear") > CURRENT_YEAR) | (F.col("deathYear") > CURRENT_YEAR),
    )
    condition_check(
        "implausible_lifespan", "name_basics", "deathYear - birthYear > 120", nb,
        (F.col("deathYear") - F.col("birthYear")) > 120,
    )
    condition_check(
        "end_before_start", "title_basics", "endYear < startYear", tb,
        F.col("endYear") < F.col("startYear"),
    )
    condition_check(
        "future_year", "title_basics", f"startYear > {CURRENT_YEAR}", tb,
        F.col("startYear") > CURRENT_YEAR,
    )
    condition_check(
        "invalid_runtime", "title_basics", "runtimeMinutes <= 0", tb,
        F.col("runtimeMinutes") <= 0,
    )
    condition_check(
        "extreme_runtime", "title_basics", "runtimeMinutes > 1000", tb,
        F.col("runtimeMinutes") > 1000,
    )
    condition_check(
        "invalid_rating", "title_ratings", "averageRating outside [1, 10]", tr,
        (F.col("averageRating") < 1) | (F.col("averageRating") > 10),
    )
    condition_check(
        "invalid_votes", "title_ratings", "numVotes <= 0", tr,
        F.col("numVotes") <= 0,
    )
    condition_check(
        "self_reference", "title_episode", "episode is its own parent", te,
        F.col("tconst") == F.col("parentTconst"),
    )

    # ---- Duplicates ----
    duplicate_check("name_basics", nb, ["nconst"])
    duplicate_check("title_basics", tb, ["tconst"])
    duplicate_check("title_ratings", tr, ["tconst"])
    duplicate_check("title_episode", te, ["tconst"])

    # ---- Foreign keys with missing parent records ----
    fk_check("title_ratings", tr, "tconst", "title_basics", tb, "tconst")
    fk_check("title_episode", te, "tconst", "title_basics", tb, "tconst")
    fk_check("title_episode", te, "parentTconst", "title_basics", tb, "tconst")
    fk_check("title_crew", tc, "tconst", "title_basics", tb, "tconst")
    fk_check("title_akas", ta, "titleId", "title_basics", tb, "tconst")
    fk_check("title_principals", tp, "tconst", "title_basics", tb, "tconst")
    fk_check("title_principals", tp, "nconst", "name_basics", nb, "nconst")
    fk_check("title_crew", exploded(tc, "directors"), "directors", "name_basics", nb, "nconst")
    fk_check("title_crew", exploded(tc, "writers"), "writers", "name_basics", nb, "nconst")
    fk_check("name_basics", exploded(nb, "knownForTitles"), "knownForTitles", "title_basics", tb, "tconst")

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("\n=== SUMMARY ===")
    for r in results:
        print(
            f"{r['issue']:20} {r['table']:17} {r['affected_rows']:>12} "
            f"{r['pct']:>9.4f}%  {r['description']}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
