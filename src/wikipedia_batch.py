import argparse
import csv
import re
from urllib.parse import unquote, urlsplit

from wikidata_lookup import find_wikipedia_articles
from wikipedia_progress import jsonl_output, write_result
from wikipedia_scraper import scrape_film


def main():
    parser = argparse.ArgumentParser(description="Fetch Wikipedia synopses for IMDb films.")
    parser.add_argument("--input", help="CSV file with a tconst column")
    parser.add_argument("--output", default="wikipedia_batch_test.jsonl")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--resume", action="store_true", help="Keep completed records in the output JSONL")
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

    with jsonl_output(args.output, ("tconst",), args.resume) as (output_file, completed):
        pending = [film_id for film_id in film_ids if (film_id,) not in completed]
        print(f"Films remaining: {len(pending)}/{len(film_ids)}", flush=True)

        for start in range(0, len(pending), args.batch_size):
            batch = pending[start:start + args.batch_size]
            articles = find_wikipedia_articles(batch)
            for film_id in batch:
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

                write_result(output_file, result)
                print(f"{film_id} - {result['note'] or result['title']}", flush=True)


if __name__ == "__main__":
    main()