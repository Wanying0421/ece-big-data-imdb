import argparse
import csv
import json
from urllib.parse import unquote, urlsplit

from wikidata_lookup import find_wikipedia_articles
from wikipedia_scraper import scrape_film


def main():
    parser = argparse.ArgumentParser(description="Collect Wikipedia film synopses.")
    parser.add_argument("--input", default="films_test.csv", help="CSV with a tconst column")
    parser.add_argument("--output", default="wikipedia_batch_test.jsonl", help="Output JSONL file")
    parser.add_argument("--batch-size", type=int, default=50, help="IMDb IDs per Wikidata request")
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    with open("films_test.csv", encoding="utf-8-sig", newline="") as input_file:
        film_ids = list(dict.fromkeys(
            row["tconst"].strip() for row in csv.DictReader(input_file)
        ))

    batch_size = 50
    articles = {}

    for start in range(0, len(film_ids), batch_size):
        batch = film_ids[start:start + batch_size]
        articles.update(find_wikipedia_articles(batch))

    with open("wikipedia_batch_test.jsonl", "w", encoding="utf-8") as output_file:
        for film_id in film_ids:
            urls = articles[film_id]

            if len(urls) == 1:
                source_url = urls[0]
                article_path = urlsplit(source_url).path.removeprefix("/wiki/")
                page_title = unquote(article_path).replace("_", " ")
                result = scrape_film(page_title, film_id)
                result["source_url"] = source_url
            else:
                result = {
                    "tconst": film_id,
                    "title": None,
                    "synopsis": None,
                    "source_url": None,
                    "note": (
                        "Multiple English Wikipedia articles found"
                        if urls else "No English Wikipedia article found"
                    ),
                }

            output_file.write(json.dumps(result, ensure_ascii=False) + "\n")
            print(f"{film_id} - {result['note'] or result['title']}")


if __name__ == "__main__":
    main()