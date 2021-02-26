import re
import logging

from bs4 import BeautifulSoup

from sites.helpers import safe_get, BadArticleFormatError


PATH_PATTERN = "\/article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)


def extract_external_id(path: str) -> int:
    result = PATH_PROG.match(path)
    if result:
        return int(result.groups()[0])
    else:
        return None


def scrape_title(soup: BeautifulSoup) -> str:
    headers = soup.find_all("h1")
    return headers[0].text.strip()


def scrape_published_at(soup: BeautifulSoup) -> str:
    PROPERTY_TAG = "article:published_time"
    tag = soup.find("meta", property=PROPERTY_TAG)
    return tag.get("content")


def scrape_article_metadata(path: str) -> dict:
    DOMAIN = "https://washingtoncitypaper.com"
    url = f"{DOMAIN}{path}"
    logging.info(f"Scraping metadata from: {url}")
    page = safe_get(url)
    soup = BeautifulSoup(page.text, features="html.parser")
    metadata = {}

    scraper_funcs = [
        ("title", scrape_title, ""),
        ("published_at", scrape_published_at, None),
    ]

    for prop, func, default in scraper_funcs:
        val = default
        try:
            val = func(soup)
        except Exception:
            logging.exception(f"Error scraping {prop} for article: {url}")
        metadata[prop] = val

    logging.info(f"Scraped metadata from: {url}")
    return metadata
