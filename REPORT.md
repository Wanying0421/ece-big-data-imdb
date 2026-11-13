# Report

## 1. Data preparation

### 1.1 Missing values per column

Full results for all 39 columns: [reports/null_rates.md](reports/null_rates.md)

Top 5 columns by percentage of missing values:

| Rank | Column | Missing % |
|---|---|---|
| 1 | title_akas.attributes | 99.4739 |
| 2 | title_basics.endYear | 98.7314 |
| 3 | name_basics.deathYear | 98.3207 |
| 4 | name_basics.birthYear | 95.6605 |
| 5 | title_principals.job | 80.6588 |

### Methodological Explanation (missing values)

TODO

## 2. Data exploration

### Q5. Series with their highest-rated episode in the first two seasons and their lowest-rated episode in the final season

We found 584 series that met both conditions. Of those, 426 still matched when we required all episodes tied for the highest rating to be in the first two seasons and all episodes tied for the lowest rating to be in the final season.

The three series with the largest gaps between their highest and lowest episode ratings were:

| Series | Highest rating | Lowest rating | Rating gap |
| --- | ---: | ---: | ---: |
| The Queen of Flow | 9.8 | 1.1 | 8.7 |
| One Punch Man | 9.5 | 1.6 | 7.9 |
| House of Cards | 9.5 | 2.5 | 7.0 |

[Download the full Q5 results (CSV)](reports/q5_results.csv)

#### Methodological explanation

We kept series with at least three seasons of ten or more episodes each and at least ten rated episodes overall. We identified the first two seasons and the final season from the available season numbers before joining the episode ratings. This way, a final season with no ratings was still treated as the final season.

For each eligible series, we calculated the highest and lowest ratings across all rated episodes with a known season number. We excluded series whose highest and lowest ratings were identical. Under the main rule, a series qualified if at least one episode with the highest rating was in the first two seasons and at least one episode with the lowest rating was in the final season. We then applied the stricter rule to check how many series still qualified when all tied episodes had to be in the expected seasons.

We excluded 2.119.096 of the 10.022.141 episode rows (21.14%) because their season number was missing. This means the results only reflect episodes that could be assigned to a season.

## 3. Data enrichment

We collected English Wikipedia synopses and biographies for the 6,822 films in the Top 10 list. We found a synopsis for 5,567 films. In total, 4,773 films had both a synopsis and biographies for three selected people.

### Results

| Measure | Count |
| --- | ---: |
| Films in the Top 10 list | 6,822 |
| Films with a synopsis | 5,567 |
| Films with selected principals | 6,819 |
| Film-person pairs processed | 20,435 |
| Distinct people searched | 10,481 |
| Distinct people with a biography | 7,829 |
| Film-person pairs with biography text | 17,739 |
| Films with at least one biography | 6,490 |
| Films with three biographies | 5,205 |
| Films with a synopsis and three biographies | 4,773 |

The same person can appear in several films, which explains why the number of film-person pairs is higher than the number of distinct people.

### Methodological explanation

We used the Top 10 film list as the reference for the synopsis collection. For biographies, we used the principals CSV provided by Wanying, generated from the cleaned IMDb data using `prepare_principals.py`. The script selects up to three distinct people per film according to IMDb's credit ordering. Three films had no selected principals, and some others had fewer than three.

For each film, we attempted to extract the Plot section of its English Wikipedia article. For each selected person, we attempted to retrieve an introductory biography. Results were saved progressively in JSONL files. The `--resume` option allowed interrupted runs to continue from saved records, and biographies were reused when the same person appeared in several films.

Before the final export, we checked the biography records against Wanying's reference CSV. All 20,435 expected film-person pairs were present, with no duplicate pairs. We excluded nine saved records that belonged only to the earlier local principals list.

### Missing information and limitations

A synopsis could not be retrieved for 1,255 films: 750 had no Plot section, 491 had no English Wikipedia article found by the matching process, 13 had an empty Plot section, and one had multiple matching articles.

Biography text was unavailable for 2,696 film-person pairs: 2,686 had no English Wikipedia article found, nine had multiple matching articles, and one had no introductory biography. These counts refer to film-person pairs, so a person appearing in several films can contribute more than once.

Missing or ambiguous results were kept as missing values with an explanatory note. The coverage therefore depends on the availability of English Wikipedia content and the matching process.

### Export and validation

We exported the combined data to Parquet using Spark and `check_enrichment.py`. Reading the exported files back confirmed 6,822 rows and 6,822 distinct film identifiers, with no missing or unexpected film identifiers.

Detailed collection counts are available in [enrichment_summary.json](reports/enrichment_summary.json). The Parquet validation results are available in [enrichment_export_validation.json](reports/enrichment_export_validation.json).


## 4. Streaming

### Architecture and Pipeline Overview
We implemented a real-time streaming data pipeline using **Apache Kafka** and **Spark Structured Streaming**:
1. **Ingestion (`kafka_producer.py`)**: A robust Python producer fetches live edit events from the Wikimedia Recent Changes stream (`https://stream.wikimedia.org/v2/stream/recentchange`) and publishes them to the `wikipedia-changes` Kafka topic. The producer includes production-grade error handling, calling `response.raise_for_status()`, explicitly closing HTTP responses in `finally` blocks, and enforcing a 5-second retry delay upon network errors (such as HTTP 429 or 503) or normal stream closures (EOF).
2. **Processing & Watermarking (`streaming_pipeline.py`)**: Spark Structured Streaming consumes events from Kafka. It parses incoming JSON payloads, filters data against IMDb titles/names, and applies a **watermarking strategy** (`withWatermark`) alongside a 10-second micro-batch trigger interval. Watermarking allows the pipeline to safely track event-time progression and drop or aggregate late-arriving data without unbounded state growth.
3. **Storage & Persistence**: Aggregated windowed metrics are continuously written to disk in Apache Parquet format (`data/streaming_output/`) using append mode.

### Validation Metrics & Execution Results
We performed a 30-minute sustained streaming run, producing 55 distinct micro-batch append cycles. 
* **Total Output Rows:** Reading back the persisted Parquet files via our validation script (`validate_output.py`) confirmed **1,597 total rows** of aggregated windowed results.
* **Watermark Progression:** Spark successfully advanced the event-time watermark across micro-batches, ensuring low-latency processing despite minor network fluctuations.
* **Sample Windowed Output:**

| window | title | count |
| :--- | :--- | :--- |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | José Revueltas | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Malbec | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Himmel | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | The Lady from Shanghai | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Readers | 3 |

*Unit testing (`src/test_kafka_producer.py`) verifies mock handling for HTTP 503 errors and normal stream EOF closure scenarios.*

## 5. Open question

### Topic: Advanced Genre Performance & Critical Reception Analysis (Since 2000)

To answer our open-ended analytical question, we investigated how modern film genres (released from 2000 onward) balance production volume, critical approval ratings, and audience vote engagement. Using PySpark, we joined cleaned `title.basics` and `title.ratings` tables, exploded multi-valued genre strings, and aggregated metrics per genre (filtering for genres with over 100 movies).

### Results & Findings

The full generated dataset is available at [reports/open_question_results.csv](reports/open_question_results.csv). Summary highlights from top and bottom categories include:

| Genre | Movie Count | Average Rating | Average Votes |
| :--- | ---: | ---: | ---: |
| News | 677 | 7.22 | 369.0 |
| Documentary | 51,254 | 7.21 | 323.0 |
| Biography | 8,806 | 7.00 | 7,683.0 |
| Drama | 88,864 | 6.24 | 5,064.0 |
| Comedy | 48,143 | 5.88 | 5,850.0 |
| Action | 20,353 | 5.74 | 15,767.0 |
| Sci-Fi | 5,748 | 5.34 | 21,362.0 |
| Horror | 21,282 | 4.87 | 4,787.0 |

### Analytical Insights
1. **Critical Reception vs. Volume:** High-volume commercial categories like **Drama** (88,864 titles) and **Comedy** (48,143 titles) form the backbone of modern releases but achieve moderate average ratings (~5.88 to 6.24). In contrast, specialized informational categories like **News** and **Documentary** achieve the highest critical averages (7.22 and 7.21 respectively).
2. **Audience Engagement (Votes):** Blockbuster genres command vastly higher viewer interaction. **Adventure** and **Sci-Fi** average over 21,000 to 25,000 votes per title, whereas critically acclaimed documentaries receive significantly fewer votes (~323 average). 
3. **Critical Underperformance:** **Horror** registers high production volume (21,282 films) alongside the lowest average rating across all genres (4.87), illustrating a high ratio of low-budget or niche releases within the category.
