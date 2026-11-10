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

## 4. Streaming

## 5. Open question
