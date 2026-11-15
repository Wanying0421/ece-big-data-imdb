# Enrichment and exploration execution commands

Run the PowerShell commands below from the repository root. Start Docker Desktop
and run `docker compose up -d` first. The Compose file mounts the repository at
`/home/jovyan/work` inside the `spark` container.

The preparation sequence must already have completed:
`load_imdb.py`, `prep_nulls.py`, `prep_quality.py`, then `prep_clean.py`.
Enrichment needs cleaned titles, ratings and principals. Q5 needs cleaned titles,
ratings and episodes. Q6 and Q7 need cleaned titles, ratings, principals and names.
All default input tables are under `data/parquet_clean/`.

Spark writes directories of CSV or Parquet files, not one file with a fixed name.
Use new output directories on each run. The commands below deliberately use a run
identifier; keep the same PowerShell terminal so the variables remain available.
No collection is needed again merely to document an already completed run.

```powershell
$runId = Get-Date -Format 'yyyyMMdd_HHmmss'
$enrichmentDir = "data/parquet/enrichment_$runId"
$q5Dir = "data/parquet/q5_$runId"
$q6Candidates = "data/parquet/q6_candidates_$runId"
$q6Languages = "data/parquet/q6_languages_$runId.jsonl"
$q6Results = "data/parquet/q6_results_$runId"
$q7Results = "data/parquet/q7_results_$runId"

New-Item -ItemType Directory -Force $enrichmentDir | Out-Null
```

## Enrichment

Create the top 10 selection after preparation:

```powershell
docker exec spark spark-submit /home/jovyan/work/src/top10.py
if ($LASTEXITCODE -ne 0) { throw 'Top 10 selection failed.' }
```

Select up to three distinct principals per film:

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/prepare_principals.py `
    --films /home/jovyan/work/reports/top10_films.csv `
    --principals /home/jovyan/work/data/parquet_clean/title_principals `
    --output "/home/jovyan/work/$enrichmentDir/principals_csv"

if ($LASTEXITCODE -ne 0) { throw 'Principal selection failed.' }

$parts = @(Get-ChildItem "$enrichmentDir/principals_csv" -Filter 'part-*.csv')
if ($parts.Count -ne 1) { throw 'Expected one Spark CSV part.' }
Copy-Item $parts[0].FullName "$enrichmentDir/principals.csv"
```

The two collection programs run on the host. Their only third-party dependency
is Beautiful Soup. Set a real contact address when running Wikimedia requests:

```powershell
py -m pip install beautifulsoup4
$env:WIKIMEDIA_CONTACT = 'YOUR_REAL_CONTACT_EMAIL'

py src/wikipedia_batch.py `
    --input reports/top10_films.csv `
    --output "$enrichmentDir/films.jsonl" `
    --resume
if ($LASTEXITCODE -ne 0) { throw 'Synopsis collection interrupted.' }

py src/wikipedia_biographies.py `
    --input "$enrichmentDir/principals.csv" `
    --output "$enrichmentDir/biographies.jsonl" `
    --resume
if ($LASTEXITCODE -ne 0) { throw 'Biography collection interrupted.' }
```

To resume, reuse the same output paths and the same input selection. Recreating
`$runId` would instead start a new run. Do not point a resume operation at a
different historical film or principal selection.

Export with Spark; this program exports the collected records and does not
perform the preceding collection steps:

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/check_enrichment.py `
    --films "/home/jovyan/work/$enrichmentDir/films.jsonl" `
    --biographies "/home/jovyan/work/$enrichmentDir/biographies.jsonl" `
    --output "/home/jovyan/work/$enrichmentDir/enriched_films"
if ($LASTEXITCODE -ne 0) { throw 'Enrichment export failed.' }
```

## Q5

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/exploration_q5.py `
    --episodes /home/jovyan/work/data/parquet_clean/title_episode `
    --ratings /home/jovyan/work/data/parquet_clean/title_ratings `
    --titles /home/jovyan/work/data/parquet_clean/title_basics `
    --output "/home/jovyan/work/$q5Dir"
if ($LASTEXITCODE -ne 0) { throw 'Q5 failed.' }
```

The program prints the number of matching series and the number satisfying its
strict tie rule. Results are in `$q5Dir/part-*.csv`.

## Q6

The Q6 implementation uses original-language statements from Wikidata P364,
matched to IMDb titles through P345. IMDb alternative-title languages are not
substituted for original film languages. Unresolved or ambiguous Wikidata matches
are recorded with a note and excluded from the language analysis.

Defaults: non-adult `movie` titles, rating at least 7.0 and at least 1,000 votes.
An actor must have successful credits on at least two distinct films with known
languages, and at least two distinct language identifiers across those films.
The three-language subset uses the same film condition and at least three
language identifiers; it does not claim those languages come from three separate
films. Language coverage and principal-credit coverage limit the results.

First export successful candidate films with Spark:

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/exploration_q6.py `
    --candidates-output "/home/jovyan/work/$q6Candidates"
if ($LASTEXITCODE -ne 0) { throw 'Q6 candidate selection failed.' }
```

Collect language statements on the host. This collector uses the Python standard
library and the repository's existing rate-limited HTTP and resumable JSONL
helpers. Retrying an interrupted run requires the same paths:

```powershell
$env:WIKIMEDIA_CONTACT = 'YOUR_REAL_CONTACT_EMAIL'

py src/collect_film_languages.py `
    --input "$q6Candidates" `
    --output "$q6Languages" `
    --resume
if ($LASTEXITCODE -ne 0) { throw 'Q6 language collection interrupted.' }
```

Run the Spark analysis with exactly the same success thresholds as candidate
selection:

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/exploration_q6.py `
    --languages "/home/jovyan/work/$q6Languages" `
    --output "/home/jovyan/work/$q6Results"
if ($LASTEXITCODE -ne 0) { throw 'Q6 analysis failed.' }
```

Output subdirectories contain `actors`, `actors_three_languages`,
`actor_languages` and `evidence` CSV tables. The language cache retains film and
language source URIs. Missing language information is not treated as evidence
that a film or an actor is monolingual.

## Q7

Defaults: non-adult `movie` credits, at least 15 distinct films and a span of at
least 20 years from the first to the last observed release year. Unrated movies
still contribute to career length and film count. The early period is the first
third of that year span, with a strict cutoff; the remainder is the later period.

The analysis uses IMDb ratings as the reception proxy requested by the example
in Q7. It retains ratings with at least 1,000 votes, requires at least three rated
movies in each period, and selects actors whose early mean and early maximum
rating both exceed the corresponding later values. A highest-rating tie across
the two periods is excluded. This is not a measure of box office or a causal
claim about career decline. The input contains principal credits, not complete
filmographies.

```powershell
docker exec spark spark-submit `
    /home/jovyan/work/src/exploration_q7.py `
    --output "/home/jovyan/work/$q7Results"
if ($LASTEXITCODE -ne 0) { throw 'Q7 failed.' }
```

The program prints a result count and sample rows, and exports all results to
`$q7Results/part-*.csv`.

Both scripts accept `--data-dir`, individual input overrides `--titles`,
`--ratings`, `--principals`, `--names`, and configurable thresholds. Input
overrides can point to raw `.tsv` or `.tsv.gz` files when cleaned Parquet tables
are unavailable. Do not mix unrelated snapshot versions without documenting it.
Use `spark-submit SCRIPT --help` inside the container for all options.

## Tests and validation status

```powershell
docker exec spark python -m pip install pytest
docker exec spark spark-submit `
    /home/jovyan/work/tests/run_exploration_tests.py
```

Four tests run on small in-memory fixtures and do not require downloaded IMDb
files or live HTTP access. They cover Q6 language coverage and ambiguity, distinct
film conditions, Q7 early versus late or tied peaks, unrated career endpoints and
duplicate principal credits. These four tests passed with PySpark 3.5.0 during
development. A live language request also succeeded for two example film IDs.
Full IMDb runs completed on 14 November 2026. Q7 produced 2,701 results. Q6 produced 2,676 actors with at least three distinct Wikidata language identifiers, including language varieties.

## Declaration for PROJECT md

The new Q6/Q7 scripts, shared input helper, language collector, tests and this
execution supplement were drafted with OpenAI ChatGPT/Codex. AI-assisted code
was checked using synthetic Spark tests and a small Wikidata lookup. The group
must perform and record the full IMDb runs. This supplement is an execution
guide; methodological explanations and reasoning for `REPORT.md` must be written
by the group under the course AI policy.

## Source documentation

- IMDb dataset dictionary: https://developer.imdb.com/non-commercial-datasets/
- Wikidata original language property: https://www.wikidata.org/wiki/Property:P364
- Wikidata IMDb identifier property: https://www.wikidata.org/wiki/Property:P345

