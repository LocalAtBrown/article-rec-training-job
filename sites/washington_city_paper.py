import re

from bs4 import BeautifulSoup

from sites.helpers import safe_get


PATH_PATTERN = "\/article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)


def extract_external_id(path: str) -> int:
    result = PATH_PROG.match(path)
    if result:
        return int(result.groups()[0])
    else:
        return None


def scrape_article_metadata(path: str) -> dict:
    DOMAIN = "https://washingtoncitypaper.com"
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
