"""Compute the percentage of null / empty values for every column.

A value counts as missing if it is NULL, or (for string columns) empty or
whitespace only. Results are printed, saved to reports/null_rates.csv and the
five columns with the highest percentage are reported.
"""

import csv

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
        return col.isNull() | (F.trim(col) == "")
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
    return [
        {
            "table": table,
            "column": field.name,
            "total_rows": total,
            "missing": row[field.name],
            "missing_pct": round(100.0 * row[field.name] / total, 4),
        }
        for field in df.schema.fields
    ]


def main():
    spark = SparkSession.builder.appName("prep_nulls").getOrCreate()

    results = []
    for table in TABLES:
        results.extend(table_null_rates(spark, table))
        print(f"done: {table}")

    with open(OUT_FILE, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    print("\n=== Missing percentage per column ===")
    for r in results:
        print(f"{r['table']:18} {r['column']:18} {r['missing_pct']:>9.4f}%")

    print("\n=== Top 5 columns by missing percentage ===")
    for r in sorted(results, key=lambda r: r["missing_pct"], reverse=True)[:5]:
        print(f"{r['table']}.{r['column']}: {r['missing_pct']}%")

    spark.stop()


if __name__ == "__main__":
    main()
