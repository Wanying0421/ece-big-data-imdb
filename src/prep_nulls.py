"""Compute the percentage of null / empty values for every column.

A value counts as missing if it is NULL, or (for string columns) empty or
whitespace only (spaces, tabs, line breaks). Results are printed, saved to
reports/null_rates.csv and the five columns with the highest percentage are
reported. Columns of an empty table are kept with a missing count of 0 and an
undefined percentage, and are left out of the top five.
"""

import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

PARQUET_DIR = "/home/jovyan/work/data/parquet"
OUT_FILE = "/home/jovyan/work/reports/null_rates.csv"

TABLES = [
    "name_basics",
    "title_akas",
    "title_basics",
    "title_crew",
    "title_episode",
    "title_principals",
    "title_ratings",
]


def missing_condition(field):
    """Spark condition that is true when the value is missing."""
    col = F.col(field.name)
    if isinstance(field.dataType, StringType):
        # \s also matches tabs and line breaks, which F.trim would leave in place.
        return col.isNull() | col.rlike(r"^\s*$")
    return col.isNull()


def table_null_rates(spark, table):
    df = spark.read.parquet(f"{PARQUET_DIR}/{table}")
    # One pass over the table: total rows and missing count for every column.
    aggs = [F.count(F.lit(1)).alias("__total__")]
    for field in df.schema.fields:
        aggs.append(
            F.sum(F.when(missing_condition(field), 1).otherwise(0)).alias(field.name)
        )
    row = df.agg(*aggs).collect()[0].asDict()
    total = row["__total__"]

    results = []
    for field in df.schema.fields:
        missing = row[field.name] or 0  # SUM over an empty table is NULL
        results.append(
            {
                "table": table,
                "column": field.name,
                "total_rows": total,
                "missing": missing,
                # Undefined (None) for an empty table.
                "missing_pct": round(100.0 * missing / total, 4) if total else None,
            }
        )
    return results


def main():
    spark = SparkSession.builder.appName("prep_nulls").getOrCreate()

    results = []
    for table in TABLES:
        results.extend(table_null_rates(spark, table))
        print(f"done: {table}")

    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)
    with open(OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("\n=== Missing percentage per column ===")
    for r in results:
        pct = "undefined" if r["missing_pct"] is None else f"{r['missing_pct']:.4f}%"
        print(f"{r['table']:18} {r['column']:18} {pct:>10}")

    print("\n=== Top 5 columns by missing percentage ===")
    defined = [r for r in results if r["missing_pct"] is not None]
    for r in sorted(defined, key=lambda r: r["missing_pct"], reverse=True)[:5]:
        print(f"{r['table']}.{r['column']}: {r['missing_pct']}%")

    spark.stop()


if __name__ == "__main__":
    main()
