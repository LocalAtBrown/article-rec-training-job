import re
from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from requests.models import Response

from lib.events import Event
from sites.helpers import (
    ArticleScrapingError,
    ScrapeFailure,
    safe_get,
    validate_response,
    validate_status_code,
)
from sites.site import Site

POPULARITY_WINDOW = 7
MAX_ARTICLE_AGE = 10
DOMAIN = "washingtoncitypaper.com"
NAME = "washington-city-paper"
FIELDS = {
    "collector_tstamp",
    "page_urlpath",
    "contexts_dev_amp_snowplow_amp_id_1",
    "event_name",
}
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


def bulk_fetch(start_date: date, end_date: date) -> List[Dict[str, Any]]:
    raise NotImplementedError


def bulk_fetch_by_article_id() -> None:
    # https://stackoverflow.com/questions/16706956/is-there-a-difference-between-raise-exception-and-raise-exception-without
    raise NotImplementedError


def get_article_text() -> None:
    raise NotImplementedError


def transform_raw_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    requires a dataframe with the following fields:
    - contexts_dev_amp_snowplow_amp_id_1
    - collector_tstamp
    - page_urlpath
    returns a dataframe with the following fields:
    - client_id
    - session_date
    - activity_time
    - landing_page_path
    - event_category (conversions, newsletter sign-ups TK)
    - event_action (conversions, newsletter sign-ups TK)
    """
    df = df.dropna(subset=["contexts_dev_amp_snowplow_amp_id_1"])
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df.contexts_dev_amp_snowplow_amp_id_1.apply(lambda x: x[0]["ampClientId"])
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp).dt.round("1s")
    transformed_df["session_date"] = pd.to_datetime(transformed_df.activity_time.dt.date)
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_name"] = df.event_name
    transformed_df.replace({"event_name": "amp_page_ping"}, Event.PAGE_PING.value, inplace=True)
    transformed_df["event_name"] = transformed_df["event_name"].astype("category")

    return transformed_df


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
            return "Article has exclude tag"

    return None


def fetch_article(external_id: str, path: str) -> Response:
    url = f"https://{DOMAIN}{path}"

    try:
        page = safe_get(url)
    except Exception as e:
        raise ArticleScrapingError(ScrapeFailure.FETCH_ERROR, path, str(external_id), f"Request failed for {url}") from e

    error_msg = validate_response(page, [validate_status_code, validate_not_excluded])
    if error_msg is not None:
        raise ArticleScrapingError(ScrapeFailure.FAILED_SITE_VALIDATION, path, str(external_id), error_msg)

    return page


WCP_SITE = Site(
    NAME,
    FIELDS,
    TRAINING_PARAMS,
    SCRAPE_CONFIG,
    transform_raw_data,
    extract_external_id,
    scrape_article_metadata,
    fetch_article,
    bulk_fetch,
    bulk_fetch_by_article_id,
    get_article_text,
    POPULARITY_WINDOW,
    MAX_ARTICLE_AGE,
)
