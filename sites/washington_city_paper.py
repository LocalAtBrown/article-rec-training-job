from typing import Optional
import re
import logging
from urllib.parse import urlparse
from requests.models import Response

from bs4 import BeautifulSoup

from sites.helpers import safe_get, ArticleScrapingError


DOMAIN = "washingtoncitypaper.com"
# supported url path formats:
# '/v/s/washingtoncitypaper.com/article/194506/10-things-you-didnt-know-about-steakumm/'
# '/article/521676/jack-evans-will-pay-2000-a-month-in-latest-ethics-settlement/'
PATH_PATTERN = f"\/((v|c)\/s\/{DOMAIN}\/)?article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)


def extract_external_id(path: str) -> int:
    result = PATH_PROG.match(path)
    if result:
        return int(result.groups()[2])
    else:
        return None


def scrape_title(page: Response, soup: BeautifulSoup) -> str:
    headers = soup.select("header h1")
    return headers[0].text.strip()


def scrape_published_at(page: Response, soup: BeautifulSoup) -> str:
    # example published_at: '2021-04-13T19:00:45+00:00'
    PROPERTY_TAG = "article:published_time"
    tag = soup.find("meta", property=PROPERTY_TAG)
    return tag.get("content") if tag is not None else None


def scrape_path(page: Response, soup: BeautifulSoup) -> str:
    # there are times when older articles redirect to an alternate path, for ex:
    # https://washingtoncitypaper.com/food/article/20830693/awardwinning-chef-michel-richard-dies-at-age-68
    return urlparse(page.url).path


def scrape_article_metadata(page: Response, soup: BeautifulSoup) -> dict:
    logging.info(f"Scraping metadata from url: {page.url}")

    metadata = {}
    scraper_funcs = [
        ("title", scrape_title),
        ("published_at", scrape_published_at),
        ("path", scrape_path),
    ]

    for prop, func in scraper_funcs:
        try:
            val = func(page, soup)
        except Exception as e:
            msg = f"Error scraping {prop} for article url: {page.url}"
            logging.exception(msg)
            raise ArticleScrapingError(msg) from e
        metadata[prop] = val

    return metadata


# TODO move all validation logic to separate file
def validate_not_excluded(page: Response, soup: BeautifulSoup) -> Optional[str]:
    error_msg = None
    primary = soup.find(id="primary")

    if not primary:
        # non-article page
        return error_msg

    classes = {
        value for element in primary.find_all(class_=True) for value in element["class"]
    }

    if "tag-exclude" in classes:
        error_msg = "Article has exclude tag"

    return error_msg


def validate_article(path: str) -> (Response, BeautifulSoup, Optional[str]):
    url = f"https://{DOMAIN}{path}"
    logging.info(f"Validating article url: {url}")

    try:
        page = safe_get(url)
    except Exception as e:
        msg = f"Error fetching article url: {url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    soup = BeautifulSoup(page.text, features="html.parser")

    error_msg = None
    validator_funcs = [
        validate_not_excluded,
    ]

    for func in validator_funcs:
        try:
            error_msg = func(page, soup)
        except Exception as e:
            error_msg = e.message
        if error_msg:
            break

    return page, soup, error_msg
