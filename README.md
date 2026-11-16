# ECE Big Data Processing Project: IMDb & Wikipedia

## Group

- Group: Data & AI gr-04
- SANTIR Nouha, git username: nouhasantir
- NGWA NGOH Laroche, git username: Laroche22
- XIONG Wanying, git username: Wanying0421

## About

Batch and streaming analysis of the [IMDb non-commercial datasets](https://developer.imdb.com/non-commercial-datasets/) with Apache Spark (PySpark) and Apache Kafka, enriched with human-authored content from English Wikipedia.

## Tech stack

- **Apache Spark 3.5 (PySpark)**: batch processing and Structured Streaming
- **Apache Kafka 3.7 (KRaft mode)**: ingestion of live Wikipedia change events
- **Docker Compose**: reproducible local environment (Spark + Kafka)
- **Wikipedia / Wikidata APIs**: film synopses and biographies, with source URLs kept as lineage

## Project parts

| Part | What it does | Main scripts |
| --- | --- | --- |
| Data preparation | Converts the 7 IMDb TSV files to Parquet, measures missing values, detects data quality issues and produces a cleaned dataset | `load_imdb.py`, `prep_nulls.py`, `prep_quality.py`, `prep_clean.py` |
| Data exploration | Answers questions Q1-Q8 (runtime outliers, popular genres, directors, series, actors, multi-talented professionals) | `top10.py`, `explore_q*.py`, `exploration_q*.py` |
| Data enrichment | Adds Wikipedia synopses for the top 10 films per genre and year (1980-2025) and biographies for three principals per film | `prepare_principals.py`, `wikipedia_batch.py`, `wikipedia_biographies.py`, `check_enrichment.py` |
| Streaming | Ingests live Wikipedia recent changes through Kafka and matches them with IMDb titles using event-time windows and watermarking | `kafka_producer.py`, `streaming_pipeline.py`, `validate_output.py` |
| Open question | Genre performance and critical reception since 2000 | `open_question.py` |

## Quick start

```bash
docker compose up -d
```

Then download the IMDb files into `data/raw/` and follow the full step-by-step guide in [PROJECT.md](PROJECT.md).

## Documents

- [PROJECT.md](PROJECT.md): contributors, step-by-step instructions to run the project from scratch, and the AI/LLM tools declaration
- [REPORT.md](REPORT.md): results tables and methodological explanations for every part

## Repository layout

| Path | Content |
| --- | --- |
| `src/` | PySpark, Kafka and Wikipedia scripts |
| `reports/` | Result tables (CSV, JSON, Markdown) referenced in `REPORT.md` |
| `tests/` | Test scripts |
| `data/` | Raw and processed data (not tracked in Git, see `PROJECT.md`) |
| `docker-compose.yml` | Spark and Kafka (KRaft) environment |
