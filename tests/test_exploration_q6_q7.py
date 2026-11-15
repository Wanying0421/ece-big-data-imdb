"""Small Spark regressions for Q6/Q7; no IMDb download or live HTTP required."""

import pytest
from pyspark.sql import SparkSession

from src.collect_film_languages import records_from_bindings
from src.exploration_helpers import actor_movies
from src.exploration_q6 import LANGUAGE_SCHEMA, analyse as q6_analyse
from src.exploration_q7 import analyse as q7_analyse


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[2]").appName("Q6 Q7 tests")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_q6_requires_multiple_films_and_excludes_ambiguous_languages(spark):
    pairs = [
        ("nm1", "tt1"), ("nm1", "tt1"), ("nm1", "tt2"),
        ("nm2", "tt3"), ("nm3", "tt4"), ("nm3", "tt5"),
        ("nm4", "tt6"), ("nm4", "tt7"), ("nm4", "tt8"),
        ("nm5", "tt9"), ("nm5", "tt10"),
    ]
    credits = spark.createDataFrame(
        [(person, film, film, 2000, 8.0, 2000) for person, film in pairs],
        "nconst string,tconst string,primaryTitle string,year int,rating double,votes long",
    )
    names = spark.createDataFrame([(f"nm{i}", f"Actor {i}") for i in range(1, 6)],
                                  "nconst string,primaryName string")
    film_languages = {
        "tt1": ["English"], "tt2": ["French"],
        "tt3": ["English", "French", "German"],
        "tt4": ["English"], "tt5": ["English"],
        "tt6": ["English"], "tt7": ["French"], "tt8": ["German"],
        "tt9": ["German"], "tt10": ["English"],
    }
    records = spark.createDataFrame([
        {"tconst": film, "source_url": "https://www.wikidata.org/wiki/Q1",
         "note": "Multiple Wikidata items found" if film == "tt9" else None,
         "languages": [{"language_id": lang, "language_name": lang,
                        "source_url": "https://www.wikidata.org/wiki/Q2"} for lang in langs]}
        for film, langs in film_languages.items()
    ], LANGUAGE_SCHEMA)
    summary, by_language, detail = q6_analyse(credits.dropDuplicates(), names, records)
    found = {row.nconst: row for row in summary.collect()}
    assert set(found) == {"nm1", "nm4"}
    assert found["nm1"].language_count == 2
    assert found["nm4"].at_least_three_languages
    assert all(row.successful_movies == 1 for row in by_language.collect())
    assert detail.count() == 5


def test_q7_counts_unrated_career_endpoints_and_excludes_late_or_tied_peaks(spark):
    rows = []
    for actor in ("early", "late", "tie", "short", "few", "unrated_ends"):
        size = 14 if actor == "few" else 15
        for i in range(size):
            year = 1980 + i * (1 if actor == "short" else 3)
            rating = 8.0 if i < 5 else 6.0
            if actor == "late" and i == 12:
                rating = 9.0
            if actor == "tie" and i == 12:
                rating = 8.0
            if actor == "unrated_ends" and i in (0, 14):
                rating = None
            rows.append((actor, f"{actor}{i}", "Film", year, rating, 2000))
    credits = spark.createDataFrame(rows,
        "nconst string,tconst string,primaryTitle string,year int,rating double,votes long")
    names = spark.createDataFrame([(name, name) for name in
        ("early", "late", "tie", "short", "few", "unrated_ends")],
        "nconst string,primaryName string")
    found = {row.nconst: row for row in q7_analyse(credits, names).collect()}
    assert set(found) == {"early", "unrated_ends"}
    assert found["unrated_ends"].first_year == 1980
    assert found["unrated_ends"].last_year == 2022
    assert found["unrated_ends"].movie_count == 15
    assert found["early"].early_rated_movies == 5


def test_actor_movies_deduplicates_credits_and_keeps_unrated_movies(spark):
    tables = {
        "titles": spark.createDataFrame([
            ("tt1", "Film", "2000", "movie", "0"),
            ("tt2", "Series", "2000", "tvSeries", "0"),
        ], "tconst string,primaryTitle string,startYear string,titleType string,isAdult string"),
        "ratings": spark.createDataFrame([], "tconst string,averageRating string,numVotes string"),
        "principals": spark.createDataFrame([
            ("tt1", "nm1", "actor"), ("tt1", "nm1", "actor"),
            ("tt1", "nm2", "director"), ("tt2", "nm1", "actor"),
        ], "tconst string,nconst string,category string"),
    }
    rows = actor_movies(tables).collect()
    assert len(rows) == 1
    assert rows[0].nconst == "nm1" and rows[0].rating is None


def test_language_lookup_does_not_invent_missing_or_ambiguous_languages():
    def binding(film, item, language=None):
        result = {"imdb_id": {"value": film},
                  "film": {"value": f"http://www.wikidata.org/entity/{item}"}}
        if language:
            result["language"] = {"value": f"http://www.wikidata.org/entity/{language}"}
            result["languageLabel"] = {"value": "English"}
        return result
    records = records_from_bindings(["tt1", "tt2", "tt3", "tt4"], [
        binding("tt1", "Q1", "Q1860"), binding("tt2", "Q2"),
        binding("tt3", "Q3", "Q1860"), binding("tt3", "Q4", "Q1860"),
    ])
    assert records[0]["languages"][0]["language_id"] == "Q1860"
    assert records[0]["source_url"].startswith("https://")
    assert records[1]["note"] == "No original language statement found"
    assert records[2]["languages"] == []
    assert records[2]["note"] == "Multiple Wikidata items found"
    assert records[3]["note"] == "No Wikidata item found"
