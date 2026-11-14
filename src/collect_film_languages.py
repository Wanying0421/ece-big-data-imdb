"""Collect Wikidata original languages for Q6 without generating any text."""

import argparse
import csv
import json
import re
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request

if __package__:
    from .wikimedia_http import open_wikimedia
    from .wikipedia_progress import jsonl_output, write_result
else:
    from wikimedia_http import open_wikimedia
    from wikipedia_progress import jsonl_output, write_result


def resolve_languages(ids):
    for film_id in ids:
        if not re.fullmatch(r"tt[0-9]+", film_id):
            raise ValueError(f"Invalid IMDb film identifier: {film_id}")
    values = " ".join(f'"{film_id}"' for film_id in ids)
    query = f"""
    SELECT DISTINCT ?imdb_id ?film ?language ?languageLabel WHERE {{
        VALUES ?imdb_id {{ {values} }}
        ?film wdt:P345 ?imdb_id .
        OPTIONAL {{
            ?film wdt:P364 ?language .
            SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
    }}
    """
    request = Request(
        "https://query.wikidata.org/sparql?" + urlencode({"query": query, "format": "json"}),
        headers={"Accept": "application/sparql-results+json"},
    )
    with open_wikimedia(request, timeout=60) as response:
        rows = json.load(response)["results"]["bindings"]
    return records_from_bindings(ids, rows)


def records_from_bindings(ids, rows):
    matches = {film_id: {} for film_id in ids}
    for row in rows:
        film_id = row["imdb_id"]["value"]
        film_url = row["film"]["value"].replace("http://", "https://", 1)
        languages = matches[film_id].setdefault(film_url, {})
        if "language" in row:
            language_url = row["language"]["value"].replace("http://", "https://", 1)
            language_id = language_url.rsplit("/", 1)[-1]
            languages[language_id] = {
                "language_id": language_id,
                "language_name": row.get("languageLabel", row["language"])["value"],
                "source_url": language_url,
            }
    records = []
    for film_id, films in matches.items():
        record = {"tconst": film_id, "languages": [], "source_url": None, "note": None}
        if not films:
            record["note"] = "No Wikidata item found"
        elif len(films) != 1:
            record["note"] = "Multiple Wikidata items found"
        else:
            record["source_url"], languages = next(iter(films.items()))
            record["languages"] = [languages[key] for key in sorted(languages)]
            if not languages:
                record["note"] = "No original language statement found"
        records.append(record)
    return records


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Candidate CSV or Spark CSV directory")
    parser.add_argument("--output", required=True, help="Language cache JSONL")
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.batch_size <= 50:
        parser.error("--batch-size must be between 1 and 50")
    source = Path(args.input)
    files = sorted(source.glob("part-*.csv")) if source.is_dir() else [source]
    if not files:
        parser.error("No candidate CSV files were found")
    film_ids = []
    for file in files:
        with file.open(encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if "tconst" not in (reader.fieldnames or []):
                parser.error("Candidate CSV must contain tconst")
            film_ids.extend(row["tconst"].strip() for row in reader)
    film_ids = list(dict.fromkeys(film_ids))
    if not film_ids:
        parser.error("Candidate list is empty")
    with jsonl_output(args.output, ("tconst",), args.resume) as (output, completed):
        pending = [film_id for film_id in film_ids if (film_id,) not in completed]
        print(f"Films with languages remaining: {len(pending)}/{len(film_ids)}", flush=True)
        for start in range(0, len(pending), args.batch_size):
            for record in resolve_languages(pending[start:start + args.batch_size]):
                write_result(output, record)
            print(f"Saved {min(start + args.batch_size, len(pending))}/{len(pending)} pending films", flush=True)


if __name__ == "__main__":
    main()
