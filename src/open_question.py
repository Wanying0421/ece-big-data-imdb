from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

def main():
    spark = SparkSession.builder \
        .appName("IMDbOpenQuestionAnalysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Load cleaned IMDb datasets
    title_basics = spark.read.parquet("work/data/parquet_clean/title_basics")
    title_ratings = spark.read.parquet("work/data/parquet_clean/title_ratings")

    # Open Question: Analyzing average rating and movie count per genre for feature films since 2000
    movies = title_basics.filter((col("titleType") == "movie") & (col("startYear") >= 2000))
    
    joined_df = movies.join(title_ratings, "tconst")

    genre_analysis = joined_df.groupBy("genres") \
        .agg(
            count("tconst").alias("movie_count"),
            avg("averageRating").alias("avg_rating")
        ) \
        .filter(col("movie_count") > 50) \
        .orderBy(col("avg_rating").desc())

    print("=== Open Question Results: Top Genres by Average Rating (Since 2000) ===")
    genre_analysis.show(10, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
