"""Convert the raw IMDb TSV files to Parquet.

Reads data/raw/*.tsv.gz, treats the string "\\N" as null,
casts columns to proper types and writes data/parquet/<table>/.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_DIR = "/home/jovyan/work/data/raw"
OUT_DIR = "/home/jovyan/work/data/parquet"

# table name -> {column: type}. Columns not listed stay as strings.
SCHEMAS = {
    "name.basics": {
        "birthYear": "int",
        "deathYear": "int",
    },
    "title.basics": {
        "isAdult": "int",
        "startYear": "int",
        "endYear": "int",
        "runtimeMinutes": "int",
    },
    "title.akas": {
        "ordering": "int",
        "isOriginalTitle": "int",
    },
    "title.crew": {},
    "title.episode": {
        "seasonNumber": "int",
        "episodeNumber": "int",
    },
    "title.principals": {
        "ordering": "int",
    },
    "title.ratings": {
        "averageRating": "double",
        "numVotes": "int",
    },
}


def main():
    spark = (
        SparkSession.builder.appName("load_imdb")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .getOrCreate()
    )

    for table, casts in SCHEMAS.items():
        df = spark.read.csv(
            f"{RAW_DIR}/{table}.tsv.gz",
            sep="\t",
            header=True,
            nullValue="\\N",
            quote="",  # IMDb files contain stray quote characters
        )
        for col, dtype in casts.items():
            df = df.withColumn(col, F.col(col).cast(dtype))

        out_path = f"{OUT_DIR}/{table.replace('.', '_')}"
        df.write.mode("overwrite").parquet(out_path)
        print(f"{table}: wrote {out_path}")

    spark.stop()


if __name__ == "__main__":
    main()
