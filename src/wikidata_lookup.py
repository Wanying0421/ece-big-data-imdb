import json
import re
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def find_wikipedia_articles(imdb_ids):
    unique_ids = list(dict.fromkeys(imdb_ids))
    if not unique_ids:
        return {}

    for imdb_id in unique_ids:
        if not re.fullmatch(r"(tt|nm)[0-9]+", imdb_id):
            raise ValueError(f"Invalid IMDb ID: {imdb_id}")

    values = " ".join(f'"{imdb_id}"' for imdb_id in unique_ids)
    query = f"""
    SELECT DISTINCT ?imdb_id ?article WHERE {{
        VALUES ?imdb_id {{ {values} }}
        ?film wdt:P345 ?imdb_id .
        ?article schema:about ?film ;
                 schema:isPartOf <https://en.wikipedia.org/> .
    }}
    """
    params = urlencode({"query": query, "format": "json"})
    request = Request(
        f"https://query.wikidata.org/sparql?{params}",
        headers={
            "User-Agent": (
                "ECE-IMDb-Enrichment/0.1 "
                "(https://github.com/Wanying0421/ece-big-data-imdb)"
            )
        },
    )

    with urlopen(request, timeout=30) as response:
        rows = json.load(response)["results"]["bindings"]

    articles = {imdb_id: [] for imdb_id in unique_ids}
    for row in rows:
        articles[row["imdb_id"]["value"]].append(row["article"]["value"])

    return articles