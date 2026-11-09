from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import argparse

parser = argparse.ArgumentParser(description="Export Wikipedia enrichment to Parquet")
parser.add_argument("--films", default="wikipedia_batch_test.jsonl", help="Film JSONL input")
parser.add_argument(
    "--biographies",
    default="wikipedia_biographies_batch.jsonl",
    help="Biography JSONL input",
)
parser.add_argument(
    "--output",
    default="data/parquet/wikipedia_enriched_sample",
    help="Parquet output directory",
)
args = parser.parse_args()

spark = SparkSession.builder.appName("Check IMDb enrichment").getOrCreate()

films = spark.read.schema(
    "tconst STRING, title STRING, synopsis STRING, source_url STRING, note STRING"
).json(args.films)

people = spark.read.schema(
    "tconst STRING, nconst STRING, name STRING, biography STRING, "
    "source_url STRING, note STRING"
).json(args.biographies)

biographies_by_film = people.groupBy("tconst").agg(
    F.countDistinct(
        F.when(F.length(F.trim("biography")) > 0, F.col("nconst"))
    ).alias("biography_count"),
    F.collect_list(
        F.struct("nconst", "name", "biography", "source_url", "note")
    ).alias("people"),
)

enriched = (
    films.join(biographies_by_film, on="tconst", how="left")
    .fillna(0, subset=["biography_count"])
    .withColumn(
        "coverage_note",
        F.when(F.col("biography_count") < 3, F.lit("Fewer than 3 biographies")),
    )
)

enriched.select("title", "biography_count", "coverage_note").show(truncate=False)

enriched.write.parquet(args.output)
spark.stop()