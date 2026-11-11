from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, current_timestamp
from pyspark.sql.types import StructType, StringType, LongType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("IMDbWikipediaStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Load cleaned IMDb titles to filter Wikipedia changes against
    title_basics = spark.read.parquet("work/data/parquet_clean/title_basics")
    valid_titles = title_basics.select(col("primaryTitle").alias("imdb_title")).distinct()

    schema = StructType() \
        .add("title", StringType()) \
        .add("timestamp", LongType()) \
        .add("user", StringType())

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "wikipedia-changes") \
        .load()

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Fix timestamp casting from epoch seconds to timestamp
    casted_df = parsed_df \
        .withColumn("event_time", col("timestamp").cast(TimestampType())) \
        .dropna(subset=["event_time", "title"])

    # Join back with IMDb titles to fulfill project requirements
    joined_df = casted_df.join(
        valid_titles,
        casted_df["title"] == valid_titles["imdb_title"],
        "inner"
    )

    watermarked_df = joined_df \
        .withWatermark("event_time", "10 minutes")

    windowed_counts = watermarked_df \
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("title")
        ) \
        .count()

    query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("parquet") \
        .option("path", "work/data/streaming_output") \
        .option("checkpointLocation", "work/data/checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()