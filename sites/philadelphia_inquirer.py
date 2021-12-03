from typing import Optional
from retrying import retry
import logging
from urllib.parse import urlparse
from requests.models import Response
from datetime import datetime
import requests
import pandas as pd
import pdb
import re

from lib.config import config
from sites.helpers import ArticleFetchError, transform_data_google_tag_manager
from sites.site import Site


DOMAIN = "www.inquirer.com"
NAME = "philadelphia-inquirer"
FIELDS = [ "collector_tstamp", "page_urlpath", "domain_userid"]

PI_ARC_API_URL = "https://api.pmn.arcpublishing.com/content/v4"
# Note I've removed this key from the commit
ARC_KEY = config.get("INQUIRER_TOKEN")
PI_ARC_API_HEADER = {"Authorization": ARC_KEY}
PI_ARC_API_SITE = "philly-media-network"
TIMEOUT_SECONDS = 30

# supported url path formats:
#- [`https://www.inquirer.com/news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html`](https://www.inquirer.com/news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html)

# /news/philadelphia-school-district-teacher-vacancies-staffing-crisis-20211128.html 


@retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
def safe_get_pi(params:dict):
    """
    """
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

def get_date(res_val:dict) -> str:
    """
    """
    res_val = datetime.strptime(res_val["publish_date"],'%Y-%m-%dT%H:%M:%S.%fZ').isoformat()
    return res_val

def get_url(res_val:dict) -> str:
    """
    """
    return res_val["canonical_url"] 

def get_headline(res_val:dict) -> str:
    """
    """
    res_val = res_val["headlines"]
    if "meta_title" in res_val:
        return res_val["meta_title"]

    return res_val["basic"]

def parse_article_metadata(page: Response, external_id: str) -> dict:
    """
    """
    logging.info(f"Parsing metadata from id: {external_id}")

    metadata = {}
    parse_keys = [
        ("title", get_headline),
        ("published_at", get_date), # example published_at: '2021-11-17T00:44:12.319Z'
        ("path", get_url)
    ]
    
    res = page.json()

    for prop, func in parse_keys:
        val = None
        try:
            val = func(res)
        except Exception as e:
            msg = f"Error parsing {prop} for article id: {external_id}"
            logging.exception(msg)
            raise ArticleFetchError(msg) from e
        metadata[prop] = val

    return metadata

def validate_not_excluded(res: Response) -> Optional[str]:
    """
    """
    try:
        res = res.json()
    except Exception as e:
        return e

    if "headlines" not in res or ("headlines" in res and "basic" not in res["headlines"]):
        return "Article missing headline"

    if "canonical_url" not in res:
        return "Article canonical URL missing"

    if "publish_date" not in res:
        return "Article publish date missing"
    
    if "publish_date" in res:
        try: 
            datetime.strptime(res["publish_date"],'%Y-%m-%dT%H:%M:%S.%fZ').isoformat()
        except Exception as e:
            return e

    return None 


def validate_article(external_id: str) -> (Response, str, Optional[str]):
    """
    """
    params = {
        "_id": external_id,
        "website": PI_ARC_API_SITE,
        "published": "true",
        "included_fields": "headlines,publish_date,_id,canonical_url"
    }

    logging.info(f"Validating article url: {external_id}")

    try:
        res = safe_get_pi(params)
    except Exception as e:
        msg = f"Error fetching article url: {url}"
        logging.exception(msg)
        raise ArticleFetchError(msg) from e

    error_msg = None
    validator_funcs = [
        validate_not_excluded,
    ]

    for func in validator_funcs:
        try:
            error_msg = func(res)
        except Exception as e:
            error_msg = e.message
        if error_msg:
            break

    return res, external_id, error_msg


PI_SITE = Site(NAME, FIELDS, transform_data_google_tag_manager, extract_external_id, parse_article_metadata, validate_article)