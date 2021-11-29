from typing import Optional
import re
import logging
from urllib.parse import urlparse
from requests.models import Response
import requests
import pandas as pd
import pdb

from sites.helpers import safe_get, ArticleFetchError, ArticleScrapingError
from sites.site import Site


DOMAIN = "www.inquirer.com"
NAME = "philadelphia-inquirer"
FIELDS = [ "collector_tstamp", "page_urlpath", "domain_userid"]

PI_ARC_API_URL = "https://api.pmn.arcpublishing.com/content/v4"
# Note I've removed this key from the commit
PI_ARC_API_HEADER = {"Authorization": ARC_KEY}
PI_ARC_API_SITE = "philly-media-network"

# supported url path formats:
#- [`https://www.inquirer.com/news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html`](https://www.inquirer.com/news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html)

# /news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html 

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
    transformed_df["client_id"] = df['domain_userid']
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp)
    transformed_df["session_date"] = pd.to_datetime(transformed_df.activity_time.dt.date)
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_category"] = "snowplow_amp_page_ping"
    transformed_df["event_category"] = transformed_df["event_category"].astype("category")
    transformed_df["event_action"] = "impression"
    transformed_df["event_action"] = transformed_df["event_action"].astype("category")

    return transformed_df


def extract_external_id(path: str) -> str:
    """
    """
    params = {
        "website_url": path,
        "website": PI_ARC_API_SITE
    }
    try:
        res = requests.get(PI_ARC_API_URL, params=params, headers=PI_ARC_API_HEADER)
        res = res.json()
        contentID = res["_id"]
        return contentID
    except Exception as e:
        msg = f"Error fetching article url: {path}"
        logging.exception(msg)
        raise ArticleFetchError(msg) from e

    return None

def scrape_title(res: Response) -> str:
    api_info = res.json()
    headers = api_info['headline']
    return headers


def scrape_published_at(res: Response) -> str:
    # example published_at: '2021-11-12T12:45:35-06:00'
    api_info=res.json()
    pub_date = api_info['pub_date']
    return pub_date

def scrape_path(page: Response) -> str:
    # there are times when older articles redirect to an alternate path, for ex:
    # https://washingtoncitypaper.com/food/article/20830693/awardwinning-chef-michel-richard-dies-at-age-68
    return urlparse(page.url).path

def parse_article_metadata(page: Response, _) -> dict:
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

def validate_article(external_id: int) -> (Response, Optional[str], Optional[str]):
    external_id = int(external_id)
    # hitting the api with 38319.0 is failing but hitting with 38319 is working

    url =f"https://{DOMAIN}/api/v2/articles/{external_id}"
    logging.info(f"Validating article url: {url}")

    try:
        page = safe_get(url)
    except Exception as e:
        msg = f"Error fetching article url: {url}"
        logging.exception(msg)
        raise ArticleScrapingError(msg) from e

    return page, None, None


PI_SITE = Site(NAME, FIELDS, transform_data_google_tag_manager, extract_external_id, parse_article_metadata, validate_article)
