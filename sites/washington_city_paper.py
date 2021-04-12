import re
import logging

from bs4 import BeautifulSoup

from sites.helpers import safe_get, BadArticleFormatError


DOMAIN = "washingtoncitypaper.com"
PATH_PATTERN = f"\/((v|c)\/s\/{DOMAIN}\/)?article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)


def extract_external_id(path: str) -> int:
    result = PATH_PROG.match(path)
    if result:
        return int(result.groups()[2])
    else:
        return None


def scrape_article_metadata(path: str) -> dict:
    url = f"https://{DOMAIN}{path}"
    logging.info(f"Scraping metadata from: {url}")
    page = safe_get(url)
    soup = BeautifulSoup(page.text, features="html.parser")
    metadata = {}

    meta_tags = [
        ("title", "og:title"),
        ("published_at", "article:published_time"),
    ]

    for name, prop in meta_tags:
        tag = soup.find("meta", property=prop)
        if not tag:
            raise BadArticleFormatError(f"Could not scrape article at path: {path}")
        metadata[name] = tag.get("content")

    logging.info(f"Scraped metadata from: {url}")
    return metadata
