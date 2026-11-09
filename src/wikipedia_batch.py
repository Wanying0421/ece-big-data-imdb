import argparse
import csv
import json
import re
from urllib.parse import unquote, urlsplit

from wikidata_lookup import find_wikipedia_articles
from wikipedia_scraper import scrape_film


def main():
    parser = argparse.ArgumentParser(description="Fetch Wikipedia synopses for IMDb films.")
    parser.add_argument("--input", help="CSV file with a tconst column")
    parser.add_argument("--output", default="wikipedia_batch_test.jsonl")
    parser.add_argument("--batch-size", type=int, default=20)
    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")

    if args.input:
        with open(args.input, encoding="utf-8-sig", newline="") as input_file:
            reader = csv.DictReader(input_file)
            if "tconst" not in (reader.fieldnames or []):
                parser.error("The input CSV must contain a tconst column")

            film_ids = []
            for line_number, row in enumerate(reader, start=2):
                film_id = (row["tconst"] or "").strip()
                if not re.fullmatch(r"tt[0-9]+", film_id):
                    parser.error(f"Invalid tconst on CSV line {line_number}")
                film_ids.append(film_id)

        film_ids = list(dict.fromkeys(film_ids))
        if not film_ids:
            parser.error("The input CSV contains no film IDs")
    else:
        film_ids = ["tt1375666", "tt0133093"]

    batch_size = args.batch_size
    articles = {}

    for start in range(0, len(film_ids), batch_size):
        batch = film_ids[start:start + batch_size]
        articles.update(find_wikipedia_articles(batch))

    with open(args.output, "w", encoding="utf-8") as output_file:
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