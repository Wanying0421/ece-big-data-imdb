from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, split, explode, round

def main():
    spark = SparkSession.builder \
        .appName("IMDbAdvancedGenreAnalysis") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    title_basics = spark.read.parquet("work/data/parquet_clean/title_basics")
    title_ratings = spark.read.parquet("work/data/parquet_clean/title_ratings")

    movies = title_basics.filter(
        (col("titleType") == "movie") & 
        (col("startYear") >= 2000) & 
        (col("genres").isNotNull()) & 
        (col("genres") != "\\N")
    )

    exploded_genres = movies.withColumn("genre", explode(split(col("genres"), ",")))

    joined_df = exploded_genres.join(title_ratings, "tconst")

    genre_analysis = joined_df.groupBy("genre") \
        .agg(
            count("tconst").alias("movie_count"),
            round(avg("averageRating"), 2).alias("avg_rating"),
            round(avg("numVotes"), 0).alias("avg_votes")
        ) \
        .filter(col("movie_count") > 100) \
        .orderBy(col("avg_rating").desc(), col("movie_count").desc())

    print("=== Advanced Open Question Results: Genre Performance (Since 2000) ===")
    genre_analysis.show(20, truncate=False)

    genre_analysis.toPandas().to_csv("work/reports/open_question_results.csv", index=False)

    spark.stop()

if __name__ == "__main__":
    main()