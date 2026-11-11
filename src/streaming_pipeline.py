from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("IMDbStreamingPipeline") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load static cleaned IMDb reference data
    title_basics = spark.read.parquet("work/data/parquet_clean/title_basics")
    movies_df = title_basics.filter(col("titleType") == "movie")

    # Ingest streaming data from Kafka topic
    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "wikipedia-changes") \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = streaming_df.selectExpr("CAST(value AS STRING) as json_str")

    # Define schema matching incoming JSON events
    schema = StructType() \
        .add("title", StringType()) \
        .add("timestamp", TimestampType()) \
        .add("user", StringType())

    parsed_df = json_df.select(
        from_json(col("json_str"), schema).alias("data")
    ).select("data.*")

    # Apply watermarking for late-arriving data (allowing up to 10 minutes lateness)
    watermarked_df = parsed_df.withWatermark("timestamp", "10 minutes")

    # Join the stream with static IMDb movies dataset
    enriched_stream = watermarked_df.join(
        movies_df,
        watermarked_df.title == movies_df.primaryTitle,
        "inner"
    ).select(
        watermarked_df.title,
        movies_df.startYear,
        movies_df.genres,
        watermarked_df.timestamp,
        current_timestamp().alias("processing_time")
    )

    # Write output to console sink for testing
# Write output to a persistent Parquet sink with checkpointing
    query = enriched_stream.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "work/data/streaming_output") \
        .option("checkpointLocation", "work/data/checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
