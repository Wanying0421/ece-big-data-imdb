# Project: ECE Big Data IMDb & Wikipedia Streaming Pipeline

## Contributors & Emails
* **SANTIR Nouha** - nouha.santir@edu.ece.fr
* **NGWA NGOH Laroche** - laroche.ngwangoh@edu.ece.fr
* **Xiong Wanying** - wanying.xiong@edu.ece.fr

## Environment Setup & Operations Guide

Run the commands below in Windows PowerShell from the repository root.
Docker Desktop must be running. Python must be installed on the host.

Spark programs run inside the `spark` container with `spark-submit`.
The Kafka producer and Wikimedia collectors run on the host.

### 1. Start the environment

```powershell
docker compose up -d
docker compose ps
```

Kafka runs in KRaft mode; no ZooKeeper service is required.

### 2. Download and prepare the IMDb data

Download the following files from https://datasets.imdbws.com/ and place them
in `data/raw/`:

- `name.basics.tsv.gz`
- `title.akas.tsv.gz`
- `title.basics.tsv.gz`
- `title.crew.tsv.gz`
- `title.episode.tsv.gz`
- `title.principals.tsv.gz`
- `title.ratings.tsv.gz`

The preparation results documented by the team used the 2026-11-07 download.
Run the preparation scripts in this order. Wait for each command to finish
successfully before running the next one.

```powershell
docker exec spark spark-submit /home/jovyan/work/src/load_imdb.py
docker exec spark spark-submit /home/jovyan/work/src/prep_nulls.py
docker exec spark spark-submit /home/jovyan/work/src/prep_quality.py
docker exec spark spark-submit /home/jovyan/work/src/prep_clean.py
```

The cleaned tables are written under `data/parquet_clean/`.

### 3. Run the streaming pipeline

In a first PowerShell terminal, install the host dependencies and start
the producer:

```powershell
py -m pip install kafka-python requests
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
py src/kafka_producer.py
```

Keep the producer running. In a second PowerShell terminal, opened at the
repository root, start the Spark streaming query:

```powershell
docker exec -w /home/jovyan spark spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    /home/jovyan/work/src/streaming_pipeline.py
```

Spark connects to Kafka through the internal address `kafka:29092`.

The query uses five-minute windows, a ten-minute watermark delay and append
mode. Results are written when windows are finalized by watermark progression;
output is therefore not immediate.

After allowing the stream to produce finalized windows, validate the output
from a third PowerShell terminal at the repository root:

```powershell
docker exec -w /home/jovyan/work spark spark-submit `
    /home/jovyan/work/src/validate_output.py
```

The working directories in these commands ensure that the streaming query
and the validator use the same `data/streaming_output/` directory.

### 4. Run data enrichment

Follow the **Enrichment** section of
[the detailed execution guide](docs/run_enrichment_and_q5_q7.md),
including the initial PowerShell variable definitions.

The guide contains the complete commands to:

1. Generate `reports/top10_films.csv` using `top10.py`.
2. Select up to three distinct principals per film using `prepare_principals.py`.
3. Copy the generated Spark CSV part to a single input CSV.
4. Install Beautiful Soup and configure `WIKIMEDIA_CONTACT`.
5. Collect film synopses using `wikipedia_batch.py --input ... --output ... --resume`.
6. Collect biographies using `wikipedia_biographies.py --input ... --output ... --resume`.
7. Export the collected JSONL records to Parquet using `check_enrichment.py`
   with explicit `--films`, `--biographies` and `--output` arguments.

`check_enrichment.py` only checks and exports collected records; it does
not collect Wikipedia content.

To resume a collection, reuse its original input and output paths.
Do not generate a new run identifier for an interrupted collection.

### 5. Run data exploration and the open question

Run Q1–Q4 and Q8 inside Spark:

```powershell
docker exec spark spark-submit /home/jovyan/work/src/explore_q1.py
docker exec spark spark-submit /home/jovyan/work/src/explore_q2.py
docker exec spark spark-submit /home/jovyan/work/src/explore_q3.py
docker exec spark spark-submit /home/jovyan/work/src/explore_q4.py --min-votes 100
docker exec spark spark-submit /home/jovyan/work/src/explore_q8.py --min-votes 100
```

For Q5, Q6 and Q7, follow their corresponding sections in
[the detailed execution guide](docs/run_enrichment_and_q5_q7.md),
including the initial variable definitions.

- Q5 requires explicit episode, rating, title and output paths.
- Q6 requires candidate export, host-side language collection and final Spark analysis.
- Q7 requires an output directory.

Use new output directories for new Spark runs. The guide uses timestamped
paths to avoid overwriting previous results.

Run the open question with its required working directory:

```powershell
docker exec -w /home/jovyan spark spark-submit `
    /home/jovyan/work/src/open_question.py
```

### 6. Run the Q6/Q7 tests

```powershell
docker exec spark python -m pip install pytest
docker exec -e PYTHONPATH=/home/jovyan/work spark spark-submit `
    /home/jovyan/work/tests/run_exploration_tests.py
```

The runner executes `tests/test_exploration_q6_q7.py` through pytest.
The four tests passed during the recorded validation.

---

## Explicit Declaration of AI/LLM Tools
During the development of this project, the team utilized various AI assistants (including Gemini, Claude, and ChatGPT) to support technical tasks:
* **Code Robustness & Refactoring:** Assisting in refining error handling.
* **Unit Testing:** Structuring mock-based unit tests to verify error handling and stream closures.
* **Code Development:** Writing data loading, cleaning and exploration scripts (e.g. `load_imdb.py`, `prep_*.py`, `top10.py`, `explore_q*.py`).
* **Documentation:** Formatting result tables in `REPORT.md` and revising the run instructions in `PROJECT.md`. All explanations and reasoning in `REPORT.md` were written by the team without AI.
* **Q6/Q7 implementation:** ChatGPT/Codex assisted with `exploration_q6.py`, `exploration_q7.py`, `exploration_helpers.py`, `collect_film_languages.py`, their tests and the execution guide. The team executed the full analyses and checked the exported results.

*All implementation logic, container operations, script executions, and final validation metrics were thoroughly reviewed and verified independently by the team.*
