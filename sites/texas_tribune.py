from typing import Optional
import re
import logging
from urllib.parse import urlparse
from requests.models import Response

from bs4 import BeautifulSoup

from sites.helpers import safe_get, ArticleScrapingError
from sites.site import Site
import pandas as pd
import time

DOMAIN = "www.texastribune.org"
NAME = "texas-tribune"
FIELDS = ["collector_tstamp", "page_urlpath", "domain_userid"]
HEADERS = {"User-Agent": "article-rec-training-job/1.0.0"}


def transform_data_google_tag_manager(df: pd.DataFrame) -> pd.DataFrame:
    """
        requires a dataframe with the following fields:
                - domain_userid
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
    transformed_df = pd.DataFrame()
    transformed_df["client_id"] = df["domain_userid"]
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp)
    transformed_df["session_date"] = pd.to_datetime(
        transformed_df.activity_time.dt.date
    )
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_category"] = "snowplow_amp_page_ping"
    transformed_df["event_category"] = transformed_df["event_category"].astype(
        "category"
    )
    transformed_df["event_action"] = "impression"
    transformed_df["event_action"] = transformed_df["event_action"].astype("category")

    return transformed_df


def extract_external_id(path: str) -> str:
    article_url = f"https://{DOMAIN}{path}"

    try:
        page = safe_get(article_url, HEADERS)
        time.sleep(5)
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


def scrape_article_metadata(page: Response, soup: BeautifulSoup) -> dict:
    logging.info(f"Scraping metadata from url: {page.url}, type is {type(page)}")
    try:
        api_info = page.json()
    except Exception as e:
        msg = f"error json parsing for article url: {page.url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e
    metadata = {}
    scraper_funcs = [
        ("title", get_title),
        ("published_at", get_published_at),
        ("path", get_path),
    ]

    for prop, func in scraper_funcs:
        try:
            val = func(api_info)
        except Exception as e:
            msg = f"Error scraping {prop} for article url: {page.url}"
            logging.exception(msg)
            raise ArticleScrapingError(msg) from e
        metadata[prop] = val

    return metadata


def validate_article(
    external_id: str,
) -> (Response, Optional[BeautifulSoup], Optional[str]):
    external_id = int(external_id)

    api_url = f"https://{DOMAIN}/api/v2/articles/{external_id}"
    logging.info(f"Validating article url: {api_url}")

    try:
        res = safe_get(api_url, HEADERS)
        time.sleep(5)
    except Exception as e:
        msg = f"Error fetching article url: {api_url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e

    return res, None, None


TT_SITE = Site(
    NAME,
    FIELDS,
    transform_data_google_tag_manager,
    extract_external_id,
    scrape_article_metadata,
    validate_article,
)
