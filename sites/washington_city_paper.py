import re
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from requests.models import Response

from sites.helpers import (
    GOOGLE_TAG_MANAGER_RAW_FIELDS,
    ArticleScrapingError,
    ScrapeFailure,
    safe_get,
    transform_data_google_tag_manager,
    validate_response,
)
from sites.site import Site

POPULARITY_WINDOW = 7
MAX_ARTICLE_AGE = 10
DOMAIN = "washingtoncitypaper.com"
NAME = "washington-city-paper"
FIELDS = GOOGLE_TAG_MANAGER_RAW_FIELDS
TRAINING_PARAMS = {
    "hl": 30,
    "embedding_dim": 144,
    "epochs": 3,
    "model": "IMF",
    "loss": "adaptive_hinge",
    "tune": False,
    "tune_params": ["epochs", "embedding_dim"],
    "tune_ranges": [[1, 5, 1], [40, 300, 20]],
}

SCRAPE_CONFIG = {
    "concurrent_requests": 1,
    "requests_per_second": 2,
}

# supported url path formats:
# '/v/s/washingtoncitypaper.com/article/194506/10-things-you-didnt-know-about-steakumm/'
# '/article/521676/jack-evans-will-pay-2000-a-month-in-latest-ethics-settlement/'
PATH_PATTERN = rf"\/((v|c)\/s\/{DOMAIN}\/)?article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)

# TODO: Once merged Site object PR, make this a WCP class attribute
ERROR_MSG_TAG_EXCLUDE = "Article has exclude tag"


def bulk_fetch(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    raise NotImplementedError


def extract_external_id(path: str) -> str:
    result = PATH_PROG.match(path)
    if result:
        return result.groups()[2]
    else:
        raise ArticleScrapingError(
            ScrapeFailure.NO_EXTERNAL_ID, path, external_id=None, msg="External ID not found in path"
        )


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


def scrape_article_metadata(page: Response, external_id: str, path: str) -> dict:
    soup = BeautifulSoup(page.text, features="html.parser")
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
            raise ArticleScrapingError(
                ScrapeFailure.MALFORMED_RESPONSE, path, external_id, f"Error scraping {prop} for article path"
            ) from e
        metadata[prop] = val

    return metadata


def validate_not_excluded(page: Response) -> Optional[str]:
    soup = BeautifulSoup(page.text, features="html.parser")
    primary = soup.find(id="primary")

    if primary:
        classes = {value for element in primary.find_all(class_=True) for value in element["class"]}
        if "tag-exclude" in classes:
            return ERROR_MSG_TAG_EXCLUDE

    return None


def fetch_article(external_id: str, path: str) -> Response:
    url = f"https://{DOMAIN}{path}"

    try:
        page = safe_get(url)
    except Exception as e:
        raise ArticleScrapingError(ScrapeFailure.FETCH_ERROR, path, str(external_id), f"Request failed for {url}") from e

    error_msg = validate_response(page, [validate_not_excluded])
    if error_msg is not None:
        raise ArticleScrapingError(ScrapeFailure.FAILED_SITE_VALIDATION, path, str(external_id), error_msg)

    return page


WCP_SITE = Site(
    NAME,
    FIELDS,
    TRAINING_PARAMS,
    SCRAPE_CONFIG,
    transform_data_google_tag_manager,
    extract_external_id,
    scrape_article_metadata,
    fetch_article,
    bulk_fetch,
    POPULARITY_WINDOW,
    MAX_ARTICLE_AGE,
)
