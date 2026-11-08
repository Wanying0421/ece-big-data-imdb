from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("Check IMDb enrichment").getOrCreate()

films = spark.read.schema(
    "tconst STRING, title STRING, synopsis STRING, source_url STRING, note STRING"
).json("wikipedia_batch_test.jsonl")

people = spark.read.schema(
    "tconst STRING, nconst STRING, name STRING, biography STRING, "
    "source_url STRING, note STRING"
).json("wikipedia_biographies_batch.jsonl")

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

enriched.write.parquet("data/parquet/wikipedia_enriched_sample")
spark.stop()