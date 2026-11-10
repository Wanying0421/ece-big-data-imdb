import argparse
import csv
from urllib.parse import unquote, urlsplit

from wikidata_lookup import find_wikipedia_articles
from wikipedia_progress import jsonl_output, write_result
from wikipedia_scraper import scrape_biography


def main():
    parser = argparse.ArgumentParser(description="Collect Wikipedia biographies.")
    parser.add_argument("--input", default="principals_test.csv", help="CSV with tconst and nconst")
    parser.add_argument("--output", default="wikipedia_biographies_test.jsonl", help="Output JSONL file")
    parser.add_argument("--resume", action="store_true", help="Keep completed records in the output JSONL")
    args = parser.parse_args()

    with open(args.input, encoding="utf-8-sig", newline="") as input_file:
        credits = list(dict.fromkeys(
            (row["tconst"].strip(), row["nconst"].strip())
            for row in csv.DictReader(input_file)
        ))

    with jsonl_output(args.output, ("tconst", "nconst"), args.resume) as (output_file, completed):
        pending = {}
        for tconst, nconst in credits:
            if (tconst, nconst) not in completed:
                pending.setdefault(nconst, []).append(tconst)
        print(f"People with credits remaining: {len(pending)}", flush=True)

        # A completed credit also checkpoints that person's biography for other films.
        cached_people = {nconst: row for (_, nconst), row in completed.items()}
        person_ids = list(pending)
        batch_size = 50
        for start in range(0, len(person_ids), batch_size):
            batch = person_ids[start:start + batch_size]
            to_fetch = [nconst for nconst in batch if nconst not in cached_people]
            articles = find_wikipedia_articles(to_fetch) if to_fetch else {}

            for nconst in batch:
                if nconst in cached_people:
                    biography = cached_people[nconst]
                else:
                    urls = articles[nconst]
                    if len(urls) == 1:
                        source_url = urls[0]
                        article_path = urlsplit(source_url).path.removeprefix("/wiki/")
                        page_title = unquote(article_path).replace("_", " ")
                        biography = scrape_biography(page_title)
                        biography["source_url"] = source_url
                    else:
                        biography = {
                            "name": None,
                            "biography": None,
                            "source_url": None,
                            "note": (
                                "Multiple English Wikipedia articles found"
                                if urls else "No English Wikipedia article found"
                            ),
                        }

                for tconst in pending[nconst]:
                    result = {**biography, "tconst": tconst, "nconst": nconst}
                    write_result(output_file, result)
                print(f"{nconst} - {biography['note'] or biography['name']}", flush=True)


if __name__ == "__main__":
    main()