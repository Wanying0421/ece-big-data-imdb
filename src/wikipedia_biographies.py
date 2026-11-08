import argparse
import csv
import json
from urllib.parse import unquote, urlsplit

from wikidata_lookup import find_wikipedia_articles
from wikipedia_scraper import scrape_biography


def main():
    parser = argparse.ArgumentParser(description="Collect Wikipedia biographies.")
    parser.add_argument("--input", default="principals_test.csv", help="CSV with tconst and nconst")
    parser.add_argument("--output", default="wikipedia_biographies_test.jsonl", help="Output JSONL file")
    args = parser.parse_args()

    with open(args.input, encoding="utf-8-sig", newline="") as input_file:
        credits = list(dict.fromkeys(
            (row["tconst"].strip(), row["nconst"].strip())
            for row in csv.DictReader(input_file)
        ))

    person_ids = list(dict.fromkeys(nconst for _, nconst in credits))
    articles = {}
    batch_size = 50

    for start in range(0, len(person_ids), batch_size):
        batch = person_ids[start:start + batch_size]
        articles.update(find_wikipedia_articles(batch))
    biographies = {}

    for nconst, urls in articles.items():
        if len(urls) == 1:
            source_url = urls[0]
            article_path = urlsplit(source_url).path.removeprefix("/wiki/")
            page_title = unquote(article_path).replace("_", " ")
            biographies[nconst] = scrape_biography(page_title)
            biographies[nconst]["source_url"] = source_url
        else:
            biographies[nconst] = {
                "name": None,
                "biography": None,
                "source_url": None,
                "note": (
                    "Multiple English Wikipedia articles found"
                    if urls else "No English Wikipedia article found"
                ),
            }

    with open(args.output, "w", encoding="utf-8") as output_file:
        for tconst, nconst in credits:
            result = {"tconst": tconst, "nconst": nconst, **biographies[nconst]}
            output_file.write(json.dumps(result, ensure_ascii=False) + "\n")
            print(f"{nconst} - {result['note'] or result['name']}")


if __name__ == "__main__":
    main()