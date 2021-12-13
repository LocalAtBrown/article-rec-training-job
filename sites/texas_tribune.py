from typing import Optional, List, Dict, Any
from datetime import datetime
import re
import logging
from urllib.parse import urlparse
from requests.models import Response
import time
from bs4 import BeautifulSoup

from sites.helpers import (
    safe_get,
    ArticleScrapingError,
    transform_data_google_tag_manager,
)
from sites.site import Site

"""
TT API documentation
https://www.notion.so/a8698dd6527140aaba8acfc29be40aa8?v=d30e06f348e94063ab4f451a345bb0d2&p=209fa6fada864bc0a1555622bb559181
"""

DOMAIN = "www.texastribune.org"
NAME = "texas-tribune"
FIELDS = ["collector_tstamp", "page_urlpath", "domain_userid"]
PARAMS = {
    "hl": 8,
    "embedding_dim": 96,
    "epochs": 10,
}


def bulk_fetch(
    start_date: datetime.date, end_date: datetime.date
) -> List[Dict[str, Any]]:
    API_URL = f"https://{DOMAIN}/api/v2/articles"
    DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
    start_date = start_date.strftime(DATE_FORMAT)
    end_date = end_date.strftime(DATE_FORMAT)

    logging.info(f"Fetching articles from {start_date} to {end_date}")
    # texas tribune publishes 5-10 articles per day
    params = {"start_date": start_date, "end_date": end_date, "limit": 100}
    res = safe_get(API_URL, params=params)
    json_res = res.json()
    metadata = [parse_metadata(article) for article in json_res["results"]]
    return metadata


def extract_external_id(path: str) -> str:
    logging.info(f"Extracting external_id for {path}")
    NON_ARTICLE_PREFIXES = [
        "/districts",
        "/employees",
        "/directory",
        "/newsletters",
        "/states",
        "/search",
        "/departments",
        "/about",
        "/series",
        "/all",
        "/departments",
        "/about",
        "/program",
        "/events",
        "/library",
        "/people",
        "/donate",
        "/topics",
        "/organizations",
        "/jobs",
        "/support-us",
        "/session",
    ]
    for prefix in NON_ARTICLE_PREFIXES:
        if path.startswith(prefix):
            msg = f"Skipping non-article path: {path}"
            logging.error(msg)
            raise ArticleScrapingError(msg)

    article_url = f"https://{DOMAIN}{path}"
    try:
        page = safe_get(article_url)
        time.sleep(1)
    except Exception as e:
        msg = f"Error fetching article url: {article_url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    soup = BeautifulSoup(page.text, features="html.parser")

    token = None
    html_content = soup.html
    matched = re.search(r"contentID: '\d+'", str(html_content))
    if matched and matched.group(0):
        token = matched.group(0)
        contentID = token.split("'")[1]
        return str(int(contentID))
    else:
        return None


def get_title(res: dict) -> str:
    title = res["headline"]
    return title


def get_published_at(res: dict) -> str:
    # example published_at: '2021-11-12T12:45:35-06:00'
    pub_date = res["pub_date"]
    return pub_date


def get_path(page: dict) -> str:
    # there are times when older articles redirect to an alternate path, for ex:
    # https://washingtoncitypaper.com/food/article/20830693/awardwinning-chef-michel-richard-dies-at-age-68
    return urlparse(page["url"]).path


def get_external_id(page: dict) -> str:
    external_id = page["id"]
    return external_id


def parse_metadata(api_info: Dict[str, Any]) -> Dict[str, Any]:
    metadata = {}
    parsers = [
        ("title", get_title),
        ("published_at", get_published_at),
        ("path", get_path),
        ("external_id", get_external_id),
    ]
    for prop, func in parsers:
        try:
            val = func(api_info)
        except Exception as e:
            msg = "error parsing metadata for article"
            raise ArticleScrapingError(msg) from e
        metadata[prop] = val

    return metadata


def scrape_article_metadata(page: Response, soup: BeautifulSoup) -> dict:
    logging.info(f"Fetching metadata from url: {page.url}, type is {type(page)}")
    try:
        api_info = page.json()
    except Exception as e:
        msg = f"error parsing json res for article url: {page.url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    metadata = parse_metadata(api_info)
    return metadata


def validate_article(
    external_id: str,
) -> (Response, Optional[BeautifulSoup], Optional[str]):
    external_id = int(external_id)

    api_url = f"https://{DOMAIN}/api/v2/articles/{external_id}"
    logging.info(f"Validating article url: {api_url}")

    try:
        res = safe_get(api_url)
        time.sleep(1)
    except Exception as e:
        msg = f"Error fetching article url: {api_url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e

    return res, None, None


TT_SITE = Site(
    NAME,
    FIELDS,
    PARAMS,
    transform_data_google_tag_manager,
    extract_external_id,
    scrape_article_metadata,
    validate_article,
    bulk_fetch,
)
