# IMDb project execution and operations guide

This repository contains IMDb preparation and exploration jobs, Wikipedia
enrichment collectors, and a Kafka and Spark Structured Streaming pipeline.
Run the commands below in PowerShell from the repository root. Query outputs
are committed under `reports`; the analytical report is [REPORT.md](REPORT.md).

## Contributors

| Contributor | Email |
| --- | --- |
| NGWA NGOH Laroche | TO COMPLETE BY CONTRIBUTOR |
| SANTIR Nouha | TO COMPLETE BY CONTRIBUTOR |
| XIONG Wanying | TO COMPLETE BY CONTRIBUTOR |

The three contributors must supply their contact addresses before submission.

## Environment

- Windows with PowerShell, Git and Python 3 available through `py`.
- Docker Desktop running Linux containers and Docker Compose v2.
- The Compose file uses Spark 3.5.0 and Kafka 3.7.0 in KRaft mode. It does not
  use ZooKeeper.
- The Spark commands below request 4 GB of driver memory. Docker needs additional
  memory for Kafka and other container processes. Allow substantial free disk
  space for compressed TSV files, Parquet tables and Spark shuffle files.
- Internet access is needed for IMDb downloads, Wikimedia requests, Python
  packages and the first Spark Kafka connector download.

For a fresh checkout:

```powershell
git clone https://github.com/Wanying0421/ece-big-data-imdb.git
Set-Location ece-big-data-imdb

New-Item -ItemType Directory -Force data/raw, reports | Out-Null

docker compose up -d
docker ps --format "table {{.Names}}\t{{.Status}}"

py -m pip install beautifulsoup4 kafka-python requests
```

The expected container names are `spark` and `kafka`. The repository is mounted
at `/home/jovyan/work` in Spark. Run Spark programs using `spark-submit` in that
container; host Python does not need PySpark.

The services expose ports 8888, 4040, 9092 and 29092. Stop conflicting instances
before starting this Compose project. If Docker reports a missing Linux engine
pipe, open Docker Desktop and wait for its engine to become ready.

## Download the seven IMDb input tables

The official distribution is https://datasets.imdbws.com/. Downloads are refreshed
daily. Record which snapshot was used; exact counts can change between runs.
The following block keeps existing final files and downloads missing files to
temporary paths before renaming them:

```powershell
$imdbTables = @(
    'name.basics', 'title.akas', 'title.basics', 'title.crew',
    'title.episode', 'title.principals', 'title.ratings'
)

foreach ($table in $imdbTables) {
    $target = "data/raw/$table.tsv.gz"
    if (-not (Test-Path $target)) {
        curl.exe --fail --location --retry 3 `
            --output "$target.part" `
            "https://datasets.imdbws.com/$table.tsv.gz"
        if ($LASTEXITCODE -ne 0) { throw "Download failed: $table" }
        Move-Item "$target.part" $target
    }
}

Get-ChildItem data/raw -Filter '*.tsv.gz' | Select-Object Name, Length
```

Keep the `.gz` files compressed; Spark reads them directly. Reuse an existing
complete snapshot when available instead of mixing downloads from different
days.

## Prepare the data

Run these four programs in order. `prepare_principals.py` is a later enrichment
step and does not replace this preparation sequence:

```powershell
$preparationScripts = @(
    'load_imdb.py',
    'prep_nulls.py',
    'prep_quality.py',
    'prep_clean.py'
)

foreach ($script in $preparationScripts) {
    docker exec spark spark-submit `
        --driver-memory 4g `
        "/home/jovyan/work/src/$script"
    if ($LASTEXITCODE -ne 0) { throw "Preparation failed: $script" }
}
```

The loader writes `data/parquet/<table>`. Cleaning writes
`data/parquet_clean/<table>` and rejected records under its `_rejected`
subdirectory. Preparation outputs include `reports/null_rates.csv`,
`reports/quality_issues.csv` and `reports/cleaning_summary.csv`.
The loader and cleaning script overwrite their generated Parquet directories;
run them when rebuilding the dataset, rather than merely to update documentation.

Check that the cleaned tables exist before starting analyses:

```powershell
docker exec spark ls /home/jovyan/work/data/parquet_clean
```

## Run exploration questions 1 to 4 and 8

The default Q1 method is `log_zscore`. Q2 is run before Q3. The default vote
threshold for Q4 is zero; the command below states that value explicitly. Q8 uses
its default threshold of 100 votes:

```powershell
docker exec spark spark-submit --driver-memory 4g `
    /home/jovyan/work/src/explore_q1.py --method log_zscore
if ($LASTEXITCODE -ne 0) { throw 'Q1 failed.' }

docker exec spark spark-submit --driver-memory 4g `
    /home/jovyan/work/src/explore_q2.py
if ($LASTEXITCODE -ne 0) { throw 'Q2 failed.' }

docker exec spark spark-submit --driver-memory 4g `
    /home/jovyan/work/src/explore_q3.py
if ($LASTEXITCODE -ne 0) { throw 'Q3 failed.' }

docker exec spark spark-submit --driver-memory 4g `
    /home/jovyan/work/src/explore_q4.py --min-votes 0
if ($LASTEXITCODE -ne 0) { throw 'Q4 failed.' }

docker exec spark spark-submit --driver-memory 4g `
    /home/jovyan/work/src/explore_q8.py --min-votes 100
if ($LASTEXITCODE -ne 0) { throw 'Q8 failed.' }
```

Each program prints results and writes its corresponding CSV files in `reports`.
Use `--help` with a script to inspect available options. Running these jobs again
may replace their committed result files with results from the current snapshot.

## Run Q5 Q6 Q7 and Wikipedia enrichment

Follow the complete commands in
[the enrichment and Q5 to Q7 supplement](docs/run_enrichment_and_q5_q7.md),
starting with its run identifier and output variables. That guide includes:

1. `top10.py` to create `reports/top10_films.csv` and
   `reports/top10_by_genre_year.csv`.
2. `prepare_principals.py` to select up to three distinct people per selected
   film and export the input CSV for biography collection.
3. Host Python commands for `wikipedia_batch.py` and
   `wikipedia_biographies.py`, with `--resume` and a Wikimedia contact value.
4. `check_enrichment.py` for Spark schema enforcement, joining and Parquet export.
5. Q5 execution and the Q6 candidate, language-collection and final-analysis steps.
6. Q7 execution and the available input and threshold overrides.

Use the same terminal and output paths when resuming a collector. A new run
identifier creates a new output location. Spark CSV and Parquet exports require
new output directories unless the script explicitly enables overwriting.

For the committed Q6 and Q7 runs, Laroche used the raw-input overrides below
because cleaned Parquet tables were unavailable on her machine. The Spark jobs
read, cast, filter and join these inputs. This is an alternative to the default
cleaned-table inputs, and counts may differ from a run on cleaned tables:

```powershell
$rawInputs = @(
    '--titles', '/home/jovyan/work/data/raw/title.basics.tsv.gz',
    '--ratings', '/home/jovyan/work/data/raw/title.ratings.tsv.gz',
    '--principals', '/home/jovyan/work/data/raw/title.principals.tsv.gz',
    '--names', '/home/jovyan/work/data/raw/name.basics.tsv.gz'
)

$rawRun = Get-Date -Format 'yyyyMMdd_HHmmss'

docker exec spark spark-submit `
    --master 'local[2]' --driver-memory 4g `
    --conf spark.sql.shuffle.partitions=16 `
    /home/jovyan/work/src/exploration_q7.py `
    @rawInputs `
    --output "/home/jovyan/work/data/parquet/q7_$rawRun"
if ($LASTEXITCODE -ne 0) { throw 'Q7 failed.' }
```

The same `@rawInputs` can be added to both Spark Q6 commands in the supplement.
Its host-side Wikidata collector does not use these Spark arguments.

The committed Q6 data includes 12,099 collected film records, 11,267 films with
usable language statements, 6,265 actor results and 2,676 results with at least
three language identifiers. Q7 has 2,701 results. These are recorded outputs,
not assertions that a future live-data run must produce identical counts.

## Run the open question

This script uses paths starting with `work/`, so its working directory must be
`/home/jovyan`:

```powershell
docker exec -w /home/jovyan spark spark-submit `
    --driver-memory 4g `
    /home/jovyan/work/src/open_question.py
if ($LASTEXITCODE -ne 0) { throw 'Open question failed.' }
```

The output is `reports/open_question_results.csv` on the host.

## Run the live streaming pipeline

Kafka clients on the host use `localhost:9092`. Spark inside Docker uses
`kafka:29092`. Create the topic once; the command is safe when it already exists:

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server kafka:29092 `
    --create --if-not-exists `
    --topic wikipedia-changes `
    --partitions 1 --replication-factor 1
if ($LASTEXITCODE -ne 0) { throw 'Topic creation failed.' }
```

Open two PowerShell terminals in the repository root. In terminal A, start Spark
with its Kafka connector. The working directory is important for this script:

```powershell
docker exec -w /home/jovyan spark spark-submit `
    --driver-memory 4g `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    /home/jovyan/work/src/streaming_pipeline.py
```

After Spark has started, run the host producer in terminal B:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
py src/kafka_producer.py
```

The producer reads the Wikimedia recent-change event stream. Spark joins event
titles to cleaned IMDb primary titles, groups them into five-minute windows
sliding every minute, and uses a ten-minute event-time watermark. Its trigger is
ten seconds and its Parquet sink uses append mode. Windows are emitted after
the watermark passes their end, so non-empty output requires matching events
and enough event-time progression; file creation alone is not a row-count check.

Allow a 15 to 30 minute observation window and inspect the actual output. The
recorded run in `REPORT.md` reported 1,597 aggregate rows; live results vary.
Use a third terminal to read the output while the containers are running:

```powershell
docker exec -w /home/jovyan/work spark spark-submit `
    /home/jovyan/work/src/validate_output.py
```

This validator intentionally runs from `/home/jovyan/work`, so its
`data/streaming_output` input resolves to the same directory as the streaming
script's `work/data/streaming_output` from `/home/jovyan`.
The host paths are `data/streaming_output` and `data/checkpoint`.
Retain the checkpoint when continuing the same query.

Stop the host producer with Ctrl+C. To stop all container work after validation,
run `docker compose stop`; use `docker compose up -d` before starting another
container command. The repository bind mount retains the files written to disk.

## Run the committed tests

Install the test dependencies inside the container. Run the producer test with
container Python and the Spark-dependent tests with their `spark-submit` runner:

```powershell
docker exec spark python -m pip install pytest requests kafka-python

docker exec -w /home/jovyan/work spark python -m pytest `
    src/test_kafka_producer.py -q
if ($LASTEXITCODE -ne 0) { throw 'Producer test failed.' }

docker exec spark spark-submit `
    /home/jovyan/work/tests/run_exploration_tests.py
if ($LASTEXITCODE -ne 0) { throw 'Q6 and Q7 tests failed.' }
```

The committed producer test covers HTTP 503 error handling and response closure.
The four Q6 and Q7 tests use synthetic inputs and cover credit duplication,
missing or ambiguous languages, distinct-film conditions, early and later
rating peaks, tied peaks, and unrated career endpoints. Laroche observed all four
Q6 and Q7 tests passing inside the Spark container.

## Outputs and operating checks

- Retain the CSV and JSON summary outputs under `reports` for review.
- `reports/q6_language_sources.jsonl` preserves the collected film-language
  source URIs. `reports/q6_evidence.csv` contains the supporting film credits.
- Large raw files, Parquet data and checkpoints are excluded by `.gitignore`.
  Share required enrichment JSONL and Parquet artifacts separately with the team.
- If an input path is missing, first check the completed preparation outputs.
  `prepare_principals.py` cannot run without `title_principals`.
- A Spark shutdown message does not establish success. Check the command exit
  code and read the output back. Spark exports also create `_SUCCESS` markers.
- Full from-scratch execution of this entire guide has not been repeated as a
  single validation run. Its commands were checked against the current script
  interfaces; the recorded Q6 and Q7 runs used the raw-input route above.

## AI assistance declaration

OpenAI ChatGPT/Codex was used in Laroche's workflow for code drafting and
debugging, command preparation, and Git troubleshooting, including Wikipedia
enrichment work and the Q6/Q7 scripts, language collector, shared helper and
tests. It was also used to draft execution documentation and prepare factual
result tables from repository outputs. The added Spark logic was checked on
synthetic fixtures, and Laroche performed the recorded full Q6/Q7 runs.

Other contributors must add their own tool usage and tasks to this declaration.
Under the course policy, final explanations and reasoning in `REPORT.md` must be
written by the group without AI. The methodological and discussion placeholders
in the new Q6/Q7 report sections must be completed by the group before submission.

## Submission checks

Complete the contributor emails and the group's report explanations. Ensure
each change has a feature branch, a pull request and a peer approval before
merging. Follow the instructor's submission instructions at
https://github.com/adaltas/ece-big-data-processing-fall-2026.
