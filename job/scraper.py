from bs4 import BeautifulSoup

from job.helpers import safe_get


DOMAIN = "https://washingtoncitypaper.com/"


def scrape_metadata(path: str) -> dict:
    url = f"{DOMAIN}{path}"
    page = safe_get(url)
    soup = BeautifulSoup(page.text, features="html.parser")
    metadata = {}

    meta_tags = [
        ("title", "og:title"),
        ("published_at", "article:published_time"),
    ]

    for name, prop in meta_tags:
        tag = soup.find("meta", property=prop)
        metadata[name] = tag.get("content")

    return metadata
