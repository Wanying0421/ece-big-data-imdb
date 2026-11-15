# Report

## 1. Data preparation

### 1.1 Missing values per column

Full results for all 39 columns: [reports/null_rates.md](reports/null_rates.md)

Top 5 columns by percentage of missing values:

| Rank | Table | Column | Missing (%) |
| ---: | --- | --- | ---: |
| 1 | title_akas | attributes | 99.4739 |
| 2 | title_basics | endYear | 98.7314 |
| 3 | name_basics | deathYear | 98.3207 |
| 4 | name_basics | birthYear | 95.6605 |
| 5 | title_principals | job | 80.6588 |

#### Methodological Explanation (missing values)

We treat NULL, \N, empty strings and whitespace‑only text as missing values. Missing percentage equals (missing rows / total rows) × 100. Since IMDb uses \N for empty data, checking only NULL would give wrong results and break cross‑table comparisons.

We considered two alternatives: counting only NULL (simple but ignores \N), or counting NULL plus \N only (follows IMDb format but misses corrupted empty entries). We chose the broader rule. The small downside is rare valid entries with spaces may be misclassified, yet this captures almost all actual missing data.

### 1.2 Data quality issues

**Impossible or suspicious values**

| Issue | Rows |
| --- | ---: |
| `name_basics`: deathYear < birthYear | 28 |
| `name_basics`: lifespan > 120 years | 2 |
| `title_basics`: startYear > 2026 | 5,862 |
| `title_basics`: runtimeMinutes <= 0 | 4 |
| `title_basics`: runtimeMinutes > 1000 | 441 |

**Foreign keys with missing parent records**

| Child column | Missing parent | Rows / ids |
| --- | --- | ---: |
| `title_episode.parentTconst` | `title_basics` | 5 |
| `title_crew.tconst` | `title_basics` | 56 |
| `title_akas.titleId` | `title_basics` | 98 |
| `title_principals.nconst` | `name_basics` | 8,348 |
| `title_crew.directors` (ids in list) | `name_basics` | 423 |
| `title_crew.writers` (ids in list) | `name_basics` | 1,148 |
| `name_basics.knownForTitles` (ids in list) | `title_basics` | 72 |

**Checks with zero issues:** ratings out of range, numVotes <= 0, duplicate primary keys, self-referencing episodes, endYear < startYear, missing parents for `title_ratings.tconst`, `title_episode.tconst` and `title_principals.tconst`.

Full results: [reports/quality_issues.csv](reports/quality_issues.csv)

### 1.3 Handling of the issues

| Action | Table | Rows / ids |
| --- | --- | ---: |
| Impossible value set to NULL and flagged in `dq_flags` | `name_basics` | 30 |
| Impossible value set to NULL and flagged in `dq_flags` | `title_basics` | 6,307 |
| Row moved to `_rejected` (missing parent) | `title_akas` | 98 |
| Row moved to `_rejected` (missing parent) | `title_crew` | 56 |
| Row moved to `_rejected` (missing parent) | `title_episode` | 5 |
| Row moved to `_rejected` (missing parent) | `title_principals` | 8,348 |
| Unknown ids removed from list columns | `title_crew`, `name_basics` | see table above |

Cleaned data: `data/parquet_clean/` (not in Git). Summary: [reports/cleaning_summary.csv](reports/cleaning_summary.csv)

#### Methodological Explanation (data quality)

We mark values impossible when they break real‑world rules, such as deathYear earlier than birthYear. We set these impossible values to NULL and add a flag in dq_flags. Rows with missing parent keys are moved to _rejected because they cannot join correctly. We also remove invalid unknown IDs from list columns to avoid broken joins.

We considered keeping all bad rows or just dropping them directly. Keeping them all would break later queries. Dropping them without tracking loses audit information. Our method balances clean usable data and clear quality records.

---

## 2. Data exploration

We define feature-length movies as titles with type = 'movie', non-adult, and runtimeMinutes ≥ 40 minutes.
Filters differ per question:

- Q1: feature-length movies, no year restriction, no minimum vote threshold
- Q2, Q3: feature-length movies within 1980–2025, no minimum vote threshold
- Q4, Q8: feature-length movies, no year restriction, with a minimum vote threshold of 100

### Top 10 films per genre and year (input for Enrichment)

| Metric | Value |
| --- | ---: |
| (year, genre) combinations | 1,027 |
| Combinations with fewer than 10 films | 68 |
| Rows | 9,832 |
| Distinct films | 6,822 |
| Genres | 25 |

Outputs: [reports/top10_by_genre_year.csv](reports/top10_by_genre_year.csv), [reports/top10_films.csv](reports/top10_films.csv)

We rank films within each (year, genre) group primarily by numVotes descending. Average rating is used as a tiebreaker, followed by tconst. We considered ranking purely by rating or purely by votes. Ranking only by votes favours popular but low-quality films, while ranking only by rating favours niche films with very few voters.

### Q1. Feature-length movies with a notably different runtime

Movies used: 455,936 (50 titles flagged `extreme_runtime` excluded). Final method: `log_zscore`.

**Comparison of detection methods** (rows = movie x genre)

| Method | Rows | Distinct movies | Longer | Shorter |
| --- | ---: | ---: | ---: | ---: |
| zscore | 6,813 | 4,779 | 6,813 | 0 |
| log_zscore | 4,953 | 3,855 | 3,358 | 1,595 |
| iqr | 35,187 | 23,983 | 29,957 | 5,230 |
| mad | 16,036 | 10,424 | 16,020 | 16 |

**Results per genre (log_zscore)**

| Genre | Movies | Lower bound (min) | Upper bound (min) | Outliers | Longer | Shorter | Outliers (%) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Action | 40,321 | 43.20 | 219.23 | 217 | 207 | 10 | 0.54 |
| Adventure | 24,009 | 40.08 | 199.74 | 256 | 253 | 3 | 1.07 |
| Animation | 8,326 | 34.82 | 181.34 | 68 | 68 | 0 | 0.82 |
| Biography | 16,094 | 33.66 | 214.69 | 54 | 54 | 0 | 0.34 |
| Comedy | 95,675 | 46.18 | 178.82 | 1,000 | 293 | 707 | 1.05 |
| Crime | 34,383 | 44.88 | 192.11 | 138 | 126 | 12 | 0.40 |
| Documentary | 116,003 | 30.04 | 176.87 | 945 | 945 | 0 | 0.81 |
| Drama | 195,983 | 42.00 | 204.31 | 569 | 541 | 28 | 0.29 |
| Family | 15,941 | 39.75 | 198.59 | 51 | 51 | 0 | 0.32 |
| Fantasy | 12,583 | 42.30 | 194.93 | 49 | 48 | 1 | 0.39 |
| Film-Noir | 877 | 48.61 | 134.05 | 1 | 1 | 0 | 0.11 |
| Game-Show | 7 | not evaluated | not evaluated | 0 | 0 | 0 | 0.00 |
| History | 13,620 | 32.49 | 240.18 | 79 | 79 | 0 | 0.58 |
| Horror | 30,517 | 45.07 | 166.15 | 425 | 140 | 285 | 1.39 |
| Music | 13,952 | 34.01 | 206.20 | 87 | 87 | 0 | 0.62 |
| Musical | 8,492 | 41.90 | 219.98 | 21 | 21 | 0 | 0.25 |
| Mystery | 16,411 | 43.78 | 189.87 | 58 | 55 | 3 | 0.35 |
| News | 1,408 | 30.61 | 177.72 | 19 | 19 | 0 | 1.35 |
| Reality-TV | 296 | 31.17 | 219.49 | 3 | 3 | 0 | 1.01 |
| Romance | 42,168 | 46.35 | 199.90 | 215 | 74 | 141 | 0.51 |
| Sci-Fi | 9,765 | 41.69 | 186.40 | 63 | 58 | 5 | 0.65 |
| Sport | 6,799 | 34.96 | 205.26 | 33 | 33 | 0 | 0.49 |
| Talk-Show | 102 | 27.55 | 246.10 | 0 | 0 | 0 | 0.00 |
| Thriller | 34,021 | 48.46 | 181.68 | 474 | 74 | 400 | 1.39 |
| War | 8,047 | 39.40 | 210.14 | 47 | 47 | 0 | 0.58 |
| Western | 6,352 | 29.57 | 179.17 | 81 | 81 | 0 | 1.28 |

Full list of outliers: [reports/q1_runtime_outliers.csv](reports/q1_runtime_outliers.csv)

Average runtime varies across genres. We store extremely short or long films as runtime outliers for further inspection.

#### Methodological Explanation (Q1)

We apply log‑zscore with threshold 3. We take the log of runtime to fix right‑skewed data, then convert bounds back to minutes.

We tested raw z‑score, IQR and MAD. Plain z‑score is skewed by extreme runtimes. While IQR and MAD are robust, log‑zscore fits movie‑length distribution better. A minor downside: log transform exaggerates small differences for short films.

### Q2. Top three most popular genres per year (1980-2025)

| Year | #1 | #2 | #3 |
| ---: | --- | --- | --- |
| 1980 | Drama | Adventure | Horror |
| 1981 | Adventure | Drama | Horror |
| 1982 | Drama | Sci-Fi | Action |
| 1983 | Drama | Action | Adventure |
| 1984 | Action | Drama | Adventure |
| 1985 | Comedy | Adventure | Drama |
| 1986 | Drama | Adventure | Comedy |
| 1987 | Drama | Comedy | Action |
| 1988 | Drama | Comedy | Action |
| 1989 | Comedy | Drama | Adventure |
| 1990 | Drama | Comedy | Crime |
| 1991 | Drama | Crime | Comedy |
| 1992 | Drama | Comedy | Crime |
| 1993 | Drama | Comedy | Crime |
| 1994 | Drama | Crime | Comedy |
| 1995 | Drama | Crime | Comedy |
| 1996 | Drama | Comedy | Crime |
| 1997 | Drama | Action | Comedy |
| 1998 | Drama | Comedy | Crime |
| 1999 | Drama | Comedy | Crime |
| 2000 | Drama | Comedy | Adventure |
| 2001 | Drama | Comedy | Adventure |
| 2002 | Drama | Adventure | Action |
| 2003 | Drama | Action | Comedy |
| 2004 | Drama | Action | Comedy |
| 2005 | Drama | Comedy | Action |
| 2006 | Drama | Comedy | Thriller |
| 2007 | Drama | Comedy | Action |
| 2008 | Drama | Action | Comedy |
| 2009 | Drama | Comedy | Action |
| 2010 | Drama | Action | Adventure |
| 2011 | Drama | Action | Comedy |
| 2012 | Drama | Action | Comedy |
| 2013 | Drama | Action | Comedy |
| 2014 | Drama | Action | Adventure |
| 2015 | Drama | Action | Adventure |
| 2016 | Drama | Action | Adventure |
| 2017 | Drama | Action | Comedy |
| 2018 | Drama | Action | Adventure |
| 2019 | Drama | Action | Comedy |
| 2020 | Drama | Comedy | Action |
| 2021 | Drama | Action | Adventure |
| 2022 | Drama | Action | Comedy |
| 2023 | Drama | Action | Comedy |
| 2024 | Drama | Action | Comedy |
| 2025 | Drama | Action | Comedy |

**How often each genre appears in the top 3**

| Genre | Years in top 3 | Years ranked #1 | First year | Last year |
| --- | ---: | ---: | ---: | ---: |
| Drama | 46 | 42 | 1980 | 2025 |
| Comedy | 34 | 2 | 1985 | 2025 |
| Action | 29 | 1 | 1982 | 2025 |
| Adventure | 16 | 1 | 1980 | 2021 |
| Crime | 9 | 0 | 1990 | 1999 |
| Horror | 2 | 0 | 1980 | 1981 |
| Sci-Fi | 1 | 0 | 1982 | 1982 |
| Thriller | 1 | 0 | 2006 | 2006 |

Full ranking of all genres: [reports/q2_genre_popularity_by_year.csv](reports/q2_genre_popularity_by_year.csv)

#### Methodological Explanation (Q2)

We define popularity as total numVotes summed per genre per year; multi-genre films are counted in all their genres, as total votes reflect overall audience engagement. We considered average rating instead, but average rating favours niche films with few voters. Our vote-based metric captures wider audience attention, yet it may be skewed by individual blockbuster hits.

### Q3. Top three genres compared with the rest

**Overall, movie level (1980-2025)**

| Group | Movies | Total votes | Mean votes | Median votes | Mean rating | Vote-weighted rating | Avg yearly movie share (%) | Avg yearly vote share (%) |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| top3 | 184,622 | 1,020,539,458 | 5,527.7 | 65 | 6.051 | 7.015 | 58.59 | 85.73 |
| rest | 158,637 | 164,089,662 | 1,034.4 | 11 | 6.345 | 6.674 | 41.41 | 14.27 |

**Genre level, averaged by decade**

| Decade | Top-3 share of genre votes (%) | Votes per top-3 genre / votes per other genre | Movies per top-3 genre | Movies per other genre |
| --- | ---: | ---: | ---: | ---: |
| 1980s | 46.14 | 5.31 | 688.8 | 135.6 |
| 1990s | 47.21 | 5.62 | 817.5 | 145.5 |
| 2000s | 41.97 | 4.81 | 1,356.5 | 285.4 |
| 2010s | 44.10 | 5.57 | 2,574.0 | 711.7 |
| 2020s | 44.10 | 5.63 | 2,866.5 | 660.8 |

**Movie level, averaged by decade**

| Decade | Group | Movie share (%) | Vote share (%) | Mean votes | Median votes | Mean rating | Vote-weighted rating |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1980s | top3 | 66.99 | 85.60 | 3,793.4 | 59.5 | 6.022 | 7.316 |
| 1980s | rest | 33.01 | 14.40 | 1,279.4 | 23.0 | 5.938 | 6.513 |
| 1990s | top3 | 66.04 | 84.71 | 7,380.8 | 95.9 | 6.004 | 7.314 |
| 1990s | rest | 33.96 | 15.29 | 2,467.5 | 11.6 | 6.085 | 6.967 |
| 2000s | top3 | 56.57 | 86.58 | 8,850.3 | 115.5 | 6.035 | 7.036 |
| 2000s | rest | 43.43 | 13.43 | 1,761.0 | 9.6 | 6.503 | 6.830 |
| 2010s | top3 | 50.22 | 86.93 | 5,654.7 | 51.0 | 6.090 | 6.953 |
| 2010s | rest | 49.78 | 13.08 | 859.1 | 10.8 | 6.381 | 6.478 |
| 2020s | top3 | 49.49 | 84.24 | 3,059.6 | 52.5 | 6.057 | 6.610 |
| 2020s | rest | 50.51 | 15.75 | 538.6 | 10.7 | 6.366 | 6.213 |

Per-year details: [reports/q3_genre_level_by_year.csv](reports/q3_genre_level_by_year.csv), [reports/q3_movie_level_by_year.csv](reports/q3_movie_level_by_year.csv)

#### Methodological Explanation (Q3)

We compare at genre level and movie level. A movie belongs to the top‑3 group if any of its genres ranks top‑3 that year. We use vote share, movie count and average rating as metrics. We considered only one analysis level. Single-level analysis loses information; our dual approach is more complete, though some films enter the top‑3 group via a minor genre.

### Q4. Directors with exactly one movie >= 8.0 and other movies averaging <= 6.0

**Sensitivity to the minimum number of votes per movie**

| Min votes | Directors (others average <= 6.0) | Directors (every other movie <= 6.0) |
| ---: | ---: | ---: |
| 0 | 1,295 | 368 |
| 10 | 1,154 | 333 |
| 100 | 169 | 47 |
| 1000 | 48 | 8 |

Final run: `--min-votes 100`, **169 directors**.

**Top 20 by votes of their >= 8.0 movie**

| Director | Movie >= 8.0 | Year | Rating | Votes | Other movies | Avg rating of others |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| James McTeigue | V for Vendetta | 2005 | 8.1 | 1,260,974 | 4 | 5.975 |
| Richard Marquand | Star Wars: Episode VI - Return of the Jedi | 1983 | 8.3 | 1,228,938 | 6 | 5.983 |
| M. Night Shyamalan | The Sixth Sense | 1999 | 8.2 | 1,176,110 | 15 | 5.867 |
| Richard Kelly | Donnie Darko | 2001 | 8.0 | 924,881 | 3 | 5.433 |
| Robert Rodriguez | Sin City | 2005 | 8.0 | 825,545 | 23 | 5.896 |
| Frank Miller | Sin City | 2005 | 8.0 | 825,545 | 2 | 5.650 |
| Martin Campbell | Casino Royale | 2006 | 8.0 | 758,235 | 20 | 5.625 |
| John G. Avildsen | Rocky | 1976 | 8.1 | 706,580 | 21 | 5.776 |
| Curtis Hanson | L.A. Confidential | 1997 | 8.2 | 666,424 | 15 | 6.000 |
| Tate Taylor | The Help | 2011 | 8.1 | 534,873 | 6 | 5.883 |
| Kirk Wise | Beauty and the Beast | 1991 | 8.0 | 511,285 | 3 | 5.433 |
| Garth Davis | Lion | 2016 | 8.0 | 270,179 | 2 | 5.650 |
| Wai Keung Lau | Infernal Affairs | 2002 | 8.0 | 139,853 | 43 | 5.719 |
| Vincent Paronnaud | Persepolis | 2007 | 8.0 | 107,456 | 4 | 5.825 |
| Billy Bob Thornton | Sling Blade | 1996 | 8.0 | 105,174 | 3 | 5.933 |
| Umesh Shukla | OMG: Oh My God! | 2012 | 8.1 | 72,429 | 6 | 5.567 |
| Vikas Bahl | Queen | 2013 | 8.1 | 72,000 | 6 | 5.833 |
| Anubhav Sinha | Article 15 | 2019 | 8.1 | 42,146 | 12 | 5.775 |
| Seth Gordon | The King of Kong: A Fistful of Quarters | 2007 | 8.0 | 38,791 | 7 | 5.700 |
| Apoorv Singh Karki | Sirf Ek Bandaa Kaafi Hai | 2023 | 8.0 | 31,511 | 2 | 4.200 |

Full list (169 directors): [reports/q4_directors.csv](reports/q4_directors.csv)

#### Methodological Explanation (Q4)

We keep directors with exactly one movie ≥8.0 and at least two other films averaging ≤6.0. A minimum‑vote filter removes noisy entries. We also tested the stricter rule where every other movie is ≤6.0. Average‑based selection finds more directors but allows some well‑rated films among their weaker works.

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

### Q8. Multi-talented film professionals

Final run: `--min-votes 100`. **20,689 multi-talented people.**

**People by talent combination**

| Talent combination | People |
| --- | ---: |
| director + writer | 14,229 |
| actor + director + writer | 2,965 |
| actor + writer | 2,637 |
| actor + director | 858 |

**Performance by role combination on the same movie** (multi-talented people only)

| Roles on the movie | Credits | Movies | Mean rating | Vote-weighted rating | Median votes |
| --- | ---: | ---: | ---: | ---: | ---: |
| actor | 61,918 | 45,365 | 5.864 | 7.131 | 903 |
| director | 27,468 | 26,105 | 5.954 | 7.020 | 637 |
| writer | 27,236 | 22,801 | 5.925 | 7.078 | 870 |
| actor + director | 1,519 | 1,492 | 5.715 | 7.126 | 473 |
| actor + writer | 5,536 | 4,987 | 5.617 | 6.993 | 705 |
| director + writer | 59,164 | 56,511 | 5.967 | 7.192 | 582 |
| actor + director + writer | 6,324 | 6,122 | 5.594 | 6.971 | 490 |

**Within-person comparison: multi-role mean rating minus single-role mean rating**

| Multi-role combination | People | Mean diff | Median diff | Better (%) | Worse (%) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Any multi-role | 13,737 | +0.020 | +0.033 | 51.3 | 48.1 |
| actor + director | 877 | -0.077 | -0.063 | 47.2 | 52.2 |
| actor + director + writer | 2,583 | -0.055 | -0.025 | 48.5 | 50.9 |
| actor + writer | 2,543 | -0.007 | +0.033 | 51.3 | 48.1 |
| director + writer | 11,107 | +0.038 | +0.036 | 51.7 | 47.7 |

Full list: [reports/q8_multitalented_people.csv](reports/q8_multitalented_people.csv)

#### Methodological Explanation (Q8)

We select multi‑talented people with at least two movies in two or more roles (director, writer, actor). We compare ratings in two ways: pooled performance by role combination, and within‑person difference between multi‑role and single‑role movies. A minimum‑votes filter removes noisy low‑vote entries. Pooled comparison risks person‑level bias; our within‑person approach controls for individual talent, though sample sizes for rare role combinations are small.

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

### Streaming Metrics
To evaluate the runtime health and ingestion performance of our streaming infrastructure, we tracked the following key metrics:
* **Throughput (Events/Sec):** The rate of incoming recent change JSON records successfully parsed and published by the Kafka producer.
* **Processing Latency (End-to-End Latency):** The lag between event generation timestamp in the Wikimedia stream and the final micro-batch write completion timestamp in Parquet.
* **Watermark Progression:** The tracking speed of Spark's event-time threshold, ensuring late data windows are efficiently closed and state memory is bounded.

### Handling Late Data
In real-time network streams, packets or events frequently arrive out of order due to network jitter or latency. We handle late-arriving records via:
* **Event-Time Watermarking:** We define an allowable lateness threshold (tracking a 10-minute window margin behind the maximum observed event time). Events arriving within this threshold are safely included in window aggregations.
* **Side Outputs / Dropping Policy:** Data arriving *after* the watermark threshold passes is classified as excessively late and dropped by Spark's state manager to prevent infinite state memory retention, maintaining strict memory bounds.

### Methodological Explanation
* **Method Chosen:** Spark Structured Streaming using event‑time watermarking alongside micro‑batch triggers
* **Why we picked this approach:** We paired Kafka’s fault‑tolerant message streaming with Spark’s stateful operators. This setup handles node failures gracefully and takes care of late‑arriving events for us through event‑time semantics, which was really important given Wikimedia’s real‑world data stream.
* **Other options we thought about:**
  * *Raw Python Asyncio loops:* We considered rolling our own streaming loop in Python, but decided against it. It would have required us to manually implement fault tolerance, scaling, and watermark state tracking, which would add lots of custom work.
  * *Processing‑Time Windows:* We also ruled out processing‑time‑based windows. Since window boundaries tie to when data gets processed instead of the actual event timestamp, system load and network delays would produce inconsistent, non‑reproducible aggregated metrics for our dataset.

### Validation Metrics & Execution Results
We performed a 30-minute sustained streaming run, producing 55 distinct micro-batch append cycles. 
- **Total Output Rows:** Reading back the persisted Parquet files via our validation script (`validate_output.py`) confirmed **1,597 total rows** of aggregated windowed results.
- **Watermark Progression:** Spark successfully advanced the event-time watermark across micro-batches, ensuring low-latency processing despite minor network fluctuations.
- **Sample Windowed Output:**

| window | title | count |
| :--- | :--- | :--- |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | José Revueltas | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Malbec | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Himmel | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | The Lady from Shanghai | 1 |
| `{2026-11-12 21:08:00, 2026-11-12 21:13:00}` | Readers | 3 |

*Unit testing (`src/test_kafka_producer.py`) verifies mock handling for HTTP 503 errors and normal stream EOF closure scenarios.*

---

## 5. Open Question

### Topic: Advanced Genre Performance & Critical Reception Analysis (Since 2000)

### Question and Motivation
* **Question:** *How do modern film genres (released from 2000 onward) balance production volume, critical approval ratings, and audience vote engagement?*
* **Why it's interesting:** Standard movie databases often report high-level trends, but understanding the quantitative tradeoff between critical acclaim (average ratings) and audience engagement (vote counts) across specific genres reveals how commercial blockbusters contrast with niche or documentary filmmaking in the 21st century.

### Methodology
Using PySpark, we joined cleaned `title.basics` and `title.ratings` tables, filtered for titles released from the year 2000 onward, exploded multi-valued genre strings, and aggregated metrics per genre (filtering for genres with over 100 movies to ensure statistical significance).

### Extra Data Used
* No external data sources were introduced; the analysis leverages strictly internal joins across the cleaned IMDb core datasets (`title.basics` and `title.ratings`).

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

### Key Takeaways
1. **Critical Reception vs. Volume:** High-volume commercial categories like **Drama** (88,864 titles) and **Comedy** (48,143 titles) form the backbone of modern releases but achieve moderate average ratings (~5.88 to 6.24). In contrast, specialized informational categories like **News** and **Documentary** achieve the highest critical averages (7.22 and 7.21 respectively).
2. **Audience Engagement (Votes):** Blockbuster genres command vastly higher viewer interaction. **Adventure** and **Sci-Fi** average over 21,000 to 25,000 votes per title, whereas critically acclaimed documentaries receive significantly fewer votes (~323 average). 
3. **Critical Underperformance:** **Horror** registers high production volume (21,282 films) alongside the lowest average rating across all genres (4.87), illustrating a high ratio of low-budget or niche releases within the category.
