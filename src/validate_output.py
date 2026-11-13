from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("ValidateStreamingOutput") \
        .getOrCreate()
    
    df = spark.read.parquet("data/streaming_output")
    
    print("--- PARQUET OUTPUT VALIDATION SUMMARY ---")
    print(f"Total Rows Read Back: {df.count()}")
    print("Sample Data Rows:")
    df.show(5, truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    main()