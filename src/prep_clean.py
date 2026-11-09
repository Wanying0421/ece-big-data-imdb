"""Clean the IMDb Parquet tables (handling of the issues found by prep_quality.py).

Input : data/parquet/<table>
Output: data/parquet_clean/<table>            cleaned tables (same names)
        data/parquet_clean/_rejected/<table>  rows removed, kept for traceability
        reports/cleaning_summary.csv          row counts before / after

Strategy (nothing is dropped silently):
  * impossible values  -> set to NULL and mark the row in the dq_flags column
  * suspicious values  -> keep the value and mark the row in dq_flags
  * orphan child rows  -> moved to _rejected (the parent record does not exist)
  * orphan ids inside comma separated lists -> removed from the list
"""

import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

IN_DIR = "/home/jovyan/work/data/parquet"
OUT_DIR = "/home/jovyan/work/data/parquet_clean"
REJECT_DIR = f"{OUT_DIR}/_rejected"
SUMMARY_FILE = "/home/jovyan/work/reports/cleaning_summary.csv"

CURRENT_YEAR = 2026
MAX_RUNTIME = 1000
MAX_LIFESPAN = 120

TABLES = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_ratings",
]


def null_int():
    return F.lit(None).cast("int")


def flags(*pairs):
    """Comma separated names of the true conditions, or NULL if none is true."""
    joined = F.concat_ws(",", *[F.when(cond, F.lit(name)) for cond, name in pairs])
    return F.when(joined != "", joined)


def orphan_ids(df, col, parent_df, parent_col):
    """Distinct ids of a comma separated list column that have no parent row."""
    child = (
        df.select(F.explode(F.split(F.col(col), ",")).alias("k"))
        .where(F.col("k").isNotNull())
        .distinct()
    )
    parent = parent_df.select(F.col(parent_col).alias("k"))
    return [r["k"] for r in child.join(parent, "k", "left_anti").collect()]


def drop_ids_from_list(df, col, bad_ids):
    """Remove the given ids from a comma separated list column."""
    if not bad_ids:
        return df
    kept = F.filter(F.split(F.col(col), ","), lambda x: ~x.isin(bad_ids))
    return df.withColumn(
        col, F.when(F.size(kept) > 0, F.array_join(kept, ",")).otherwise(F.lit(None).cast("string"))
    )


def split_by_parent(child, child_key, parent_df, parent_key):
    """Return (rows whose key exists in the parent table, rows whose key does not)."""
    parent = parent_df.select(F.col(parent_key).alias(child_key))
    return (
        child.join(parent, child_key, "left_semi"),
        child.join(parent, child_key, "left_anti"),
    )


def clean_name_basics(nb, bad_titles):
    df = nb.withColumn(
        "dq_flags",
        flags(
            (F.col("deathYear") < F.col("birthYear"), "death_before_birth"),
            ((F.col("deathYear") - F.col("birthYear")) > MAX_LIFESPAN, "implausible_lifespan"),
        ),
    )
    # We cannot tell which of the two years is wrong, so both are set to NULL.
    for c in ["birthYear", "deathYear"]:
        df = df.withColumn(
            c, F.when(F.col("dq_flags").isNotNull(), null_int()).otherwise(F.col(c))
        )
    return drop_ids_from_list(df, "knownForTitles", bad_titles)


def clean_title_basics(tb):
    return tb.withColumn(
        "dq_flags",
        flags(
            (F.col("startYear") > CURRENT_YEAR, "future_start_year"),
            (F.col("runtimeMinutes") > MAX_RUNTIME, "extreme_runtime"),
            (F.col("runtimeMinutes") <= 0, "invalid_runtime"),
        ),
    ).withColumn(
        "runtimeMinutes",
        F.when(F.col("runtimeMinutes") <= 0, null_int()).otherwise(F.col("runtimeMinutes")),
    )


def main():
    spark = (
        SparkSession.builder.appName("prep_clean")
        .config("spark.sql.shuffle.partitions", "64")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    t = {name: spark.read.parquet(f"{IN_DIR}/{name}") for name in TABLES}
    nb, tb = t["name_basics"], t["title_basics"]

    # Small sets of ids that are referenced but do not exist (collected to the driver).
    bad_people = orphan_ids(t["title_crew"], "directors", nb, "nconst") + orphan_ids(
        t["title_crew"], "writers", nb, "nconst"
    )
    bad_titles = orphan_ids(nb, "knownForTitles", tb, "tconst")
    print(f"orphan person ids in crew lists: {len(bad_people)}")
    print(f"orphan title ids in knownForTitles: {len(bad_titles)}")

    out, rejected = {}, {}
    out["name_basics"] = clean_name_basics(nb, bad_titles)
    out["title_basics"] = clean_title_basics(tb)
    out["title_ratings"] = t["title_ratings"]
    out["title_episode"], rejected["title_episode"] = split_by_parent(
        t["title_episode"], "parentTconst", tb, "tconst"
    )
    crew, rejected["title_crew"] = split_by_parent(t["title_crew"], "tconst", tb, "tconst")
    crew = drop_ids_from_list(crew, "directors", bad_people)
    out["title_crew"] = drop_ids_from_list(crew, "writers", bad_people)
    out["title_akas"], rejected["title_akas"] = split_by_parent(
        t["title_akas"], "titleId", tb, "tconst"
    )
    out["title_principals"], rejected["title_principals"] = split_by_parent(
        t["title_principals"], "nconst", nb, "nconst"
    )

    for name, df in out.items():
        df.write.mode("overwrite").parquet(f"{OUT_DIR}/{name}")
        print(f"cleaned: {name}")
    for name, df in rejected.items():
        df.write.mode("overwrite").parquet(f"{REJECT_DIR}/{name}")
        print(f"rejected rows saved: {name}")

    # Summary (reading Parquet back, counts come cheaply from the file footers).
    rows = []
    for name in TABLES:
        clean = spark.read.parquet(f"{OUT_DIR}/{name}")
        flagged = (
            clean.where(F.col("dq_flags").isNotNull()).count()
            if "dq_flags" in clean.columns
            else 0
        )
        rows.append(
            {
                "table": name,
                "rows_in": spark.read.parquet(f"{IN_DIR}/{name}").count(),
                "rows_clean": clean.count(),
                "rows_rejected": (
                    spark.read.parquet(f"{REJECT_DIR}/{name}").count()
                    if name in rejected
                    else 0
                ),
                "rows_flagged": flagged,
            }
        )

    os.makedirs(os.path.dirname(SUMMARY_FILE), exist_ok=True)
    with open(SUMMARY_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print("\n=== CLEANING SUMMARY ===")
    for r in rows:
        print(
            f"{r['table']:18} in={r['rows_in']:>11} clean={r['rows_clean']:>11} "
            f"rejected={r['rows_rejected']:>7} flagged={r['rows_flagged']:>7}"
        )
    spark.stop()


if __name__ == "__main__":
    main()
