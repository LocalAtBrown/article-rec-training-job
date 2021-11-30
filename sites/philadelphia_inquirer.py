from typing import Optional
import re
from retrying import retry
import logging
from urllib.parse import urlparse
from requests.models import Response
import requests
import pandas as pd
import pdb

from sites.helpers import ArticleFetchError
from sites.site import Site


DOMAIN = "www.inquirer.com"
NAME = "philadelphia-inquirer"
FIELDS = [ "collector_tstamp", "page_urlpath", "domain_userid"]

PI_ARC_API_URL = "https://api.pmn.arcpublishing.com/content/v4"
# Note I've removed this key from the commit
ARC_KEY = j
PI_ARC_API_HEADER = {"Authorization": ARC_KEY}
PI_ARC_API_SITE = "philly-media-network"
TIMEOUT_SECONDS = 30

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

@retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
def safe_get_pi(params:dict):
    res = requests.get(PI_ARC_API_URL, 
                        timeout=TIMEOUT_SECONDS,
                        params=params, 
                        headers=PI_ARC_API_HEADER)
    return res

def extract_external_id(path: str) -> Optional[str]:
    """
    """
    params = {
        "website_url": path,
        "published": "true",
        "website": PI_ARC_API_SITE,
        "included_fields": "_id"
    }

    if "/author/" in path:
        return None

    try:
        res = safe_get_pi(params)
        res = res.json()
        contentID = res["_id"]
        return contentID
    except Exception as e:
        msg = f"Error fetching article url: {path}"
        logging.exception(msg)
        raise ArticleFetchError(msg) from e

    return None

def parse_article_metadata(page: Response, external_id: str) -> dict:
    logging.info(f"Parsing metadata from id: {external_id}")

    metadata = {}
    parse_keys = [
        ("title", "meta_title"),
        ("published_at", "publish_date"), # example published_at: '2021-11-17T00:44:12.319Z'
        ("path", "canonical_url")
    ]
    try:
        res = page.json()
    except Exception as e:
        msg = f"Error parsing json response for article id: {external_id}"
        raise ArticleFetchError(msg) from e

    for prop, key in parse_keys:
        try:
            val = res[key] 
        except Exception as e:
            msg = f"Error parsing {prop} for article id: {external_id}"
            logging.exception(msg)
            raise ArticleFetchError(msg) from e
        metadata[prop] = val

    return metadata

def validate_article(external_id: str) -> (Response, str, Optional[str]):

    params = {
        "_id": external_id,
        "website": PI_ARC_API_SITE,
        "published": "true",
        "included_fields": "meta_title,publish_date,_id,canonical_url"
    }

    logging.info(f"Validating article url: {external_id}")

    try:
        res = safe_get_pi(params)
    except Exception as e:
        msg = f"Error fetching article url: {url}"
        logging.exception(msg)
        raise ArticleFetchError(msg) from e

    return res, external_id, None


PI_SITE = Site(NAME, FIELDS, transform_data_google_tag_manager, extract_external_id, parse_article_metadata, validate_article)
