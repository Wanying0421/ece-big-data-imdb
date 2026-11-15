# Project: ECE Big Data IMDb & Wikipedia Streaming Pipeline

## Contributors & Emails
* **SANTIR Nouha** - nouha.santir@edu.ece.fr
* **NGWA NGOH Laroche** - laroche.ngwangoh@edu.ece.fr
* **Xiong Wanying** - wanying.xiong@edu.ece.fr

## Environment Setup & Operations Guide

> **NOTE:** All code and pipelines must be runnable from scratch by following the sequential instructions below.
> On Windows Git Bash, keep the `MSYS_NO_PATHCONV=1` prefix; on macOS/Linux it can be omitted.

### 1. Prerequisites & Environment Setup
Ensure you have **Docker** and **Docker Compose** installed on your machine. Clean any leftover containers and start the containerized big data environment (Apache Spark and Kafka running in **KRaft mode**):
```bash
docker compose down -v
docker compose up -d
```

### 2. Data Preparation & Cleaning
Download the raw IMDb dataset TSV files from IMDb Datasets https://datasets.imdbws.com/ (we used the 2026-11-07 download) and place them into the `data/raw/` directory.

Run the complete sequence of preparation and cleaning scripts inside the Spark container using spark-submit:
```bash
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/load_imdb.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/prep_nulls.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/prep_quality.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/prep_clean.py
```

### 3. Real-Time Streaming Pipeline (Kafka & Spark)
Start the Kafka Producer: Install required host dependencies and ingest real-time Wikipedia recent change events into the Kafka topic.

```bash
pip install kafka-python requests
python src/kafka_producer.py
```
Run the Spark Structured Streaming Job: Consume events from Kafka, filter against IMDb titles, apply watermarking, and include the required Kafka SQL package. Run this inside the Spark container:

```bash
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 /home/jovyan/work/src/streaming_pipeline.py
```
Validate Streaming Output: Verify Parquet outputs and row counts inside the Spark container:
```bash
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/validate_output.py
```

### 4. Data Enrichment
First build the Top 10 film list and select up to three principals per film:
```bash
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/top10.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/prepare_principals.py --films /home/jovyan/work/reports/top10_films.csv --principals /home/jovyan/work/data/parquet_clean/title_principals --output /home/jovyan/work/data/enrichment/principals_top10
```
Then collect English Wikipedia batch data and biographies for the reference film list, and execute the enrichment check script inside the Spark container:
```bash
python src/wikipedia_batch.py
python src/wikipedia_biographies.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/check_enrichment.py
```

### 5. Advanced Data Exploration & Open Question
Execute the core exploration and query scripts inside the Spark container:
```bash
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/open_question.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/explore_q1.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/explore_q2.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/explore_q3.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/explore_q4.py --min-votes 100
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/exploration_q5.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/exploration_q6.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/exploration_q7.py
MSYS_NO_PATHCONV=1 docker exec -it spark spark-submit /home/jovyan/work/src/explore_q8.py --min-votes 100
```

### 6. Running Test Suites (Optional)
If you want to execute the validation tests inside the Spark container, install `pytest` and run the test scripts:
```bash
MSYS_NO_PATHCONV=1 docker exec -it spark pip install pytest
MSYS_NO_PATHCONV=1 docker exec -it -e PYTHONPATH=/home/jovyan/work spark spark-submit /home/jovyan/work/tests/run_exploration_tests.py
MSYS_NO_PATHCONV=1 docker exec -it -e PYTHONPATH=/home/jovyan/work spark spark-submit /home/jovyan/work/tests/test_exploration_q6_q7.py
```

---

## Explicit Declaration of AI/LLM Tools
During the development of this project, the team utilized various AI assistants (including Gemini, Claude, and ChatGPT) to support technical tasks:
* **Code Robustness & Refactoring:** Assisting in refining error handling.
* **Unit Testing:** Structuring mock-based unit tests to verify error handling and stream closures.
* **Code Development:** Writing data loading, cleaning and exploration scripts (e.g. `load_imdb.py`, `prep_*.py`, `top10.py`, `explore_q*.py`).
* **Documentation:** Formatting result tables in `REPORT.md` and revising the run instructions in `PROJECT.md`. All explanations and reasoning in `REPORT.md` were written by the team without AI.

*All implementation logic, container operations, script executions, and final validation metrics were thoroughly reviewed and verified independently by the team.*
