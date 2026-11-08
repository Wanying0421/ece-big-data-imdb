import argparse
import json
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup

def fetch_wikipedia(**params):
    query = urlencode({"action": "parse", "format": "json", **params})
    request = Request(
        f"https://en.wikipedia.org/w/api.php?{query}",
        headers={
            "User-Agent": (
                "ECE-IMDb-Enrichment/0.1 "
                "(https://github.com/Wanying0421/ece-big-data-imdb)"
            )
        },
    )

    with urlopen(request, timeout=20) as response:
        data = json.load(response)

    if "error" in data:
        raise RuntimeError(data["error"]["info"])

    return data["parse"]


def scrape_film(page_title, tconst):
    article_path = quote(page_title.replace(" ", "_"), safe="")
    result = {
        "tconst": tconst,
        "title": page_title,
        "synopsis": None,
        "source_url": f"https://en.wikipedia.org/wiki/{article_path}",
        "note": None,
    }

    sections = fetch_wikipedia(page=page_title, prop="sections")["sections"]
    plot_index = next(
        (section["index"] for section in sections if section["line"] == "Plot"),
        None,
    )

    if plot_index is None:
        result["note"] = "No Plot section found"
        return result

    page = fetch_wikipedia(page=page_title, prop="text", section=plot_index)
    soup = BeautifulSoup(page["text"]["*"], "html.parser")

    for citation in soup.select("sup.reference"):
        citation.decompose()

    paragraphs = [p.get_text().strip() for p in soup.find_all("p")]
    synopsis = "\n\n".join(paragraph for paragraph in paragraphs if paragraph)

    if synopsis:
        result["synopsis"] = synopsis
    else:
        result["note"] = "Plot section is empty"

    return result


def scrape_biography(page_title):
    page = fetch_wikipedia(page=page_title, prop="text", section=0)
    soup = BeautifulSoup(page["text"]["*"], "html.parser")

    for element in soup.select("sup.reference, table, figure"):
        element.decompose()

    paragraphs = [p.get_text().strip() for p in soup.find_all("p")]
    biography = "\n\n".join(paragraph for paragraph in paragraphs if paragraph)
    article_path = quote(page_title.replace(" ", "_"), safe="")

    return {
        "name": page_title,
        "biography": biography or None,
        "source_url": f"https://en.wikipedia.org/wiki/{article_path}",
        "note": None if biography else "No introductory biography found",
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch a film synopsis from Wikipedia.")
    parser.add_argument("page_title", help="Exact title of the English Wikipedia article")
    parser.add_argument("--tconst", required=True, help="IMDb title identifier")
    args = parser.parse_args()

    result = scrape_film(args.page_title, args.tconst)

    with open("wikipedia_sample.jsonl", "a", encoding="utf-8") as output_file:
        output_file.write(json.dumps(result, ensure_ascii=False) + "\n")

    print(f"Saved {result['title']} to wikipedia_sample.jsonl")