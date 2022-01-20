from typing import Optional, List, Dict, Any
from retrying import retry
import logging
from urllib.parse import urlparse
from requests.models import Response
from datetime import datetime
import requests
import re

from lib.config import config
from sites.helpers import (
    GOOGLE_TAG_MANAGER_RAW_FIELDS,
    ArticleScrapingError,
    transform_data_google_tag_manager,
    safe_get,
)
from sites.site import Site

"""
ARC API documentation
https://www.notion.so/a8698dd6527140aaba8acfc29be40aa8?v=d30e06f348e94063ab4f451a345bb0d2&p=209fa6fada864bc0a1555622bb559181
"""

DOMAIN = "www.inquirer.com"
NAME = "philadelphia-inquirer"
FIELDS = GOOGLE_TAG_MANAGER_RAW_FIELDS

API_URL = "https://api.pmn.arcpublishing.com/content/v4"
API_KEY = config.get("INQUIRER_TOKEN")
API_HEADER = {"Authorization": API_KEY}
API_SITE = "philly-media-network"
PARAMS = {
    "hl": 120,
    "embedding_dim": 256,
    "epochs": 2,
    "tune": False,
    "tune_params": ["epochs", "embedding_dim"],
    "tune_ranges": [[1, 3, 1], [30, 180, 50]],
    "model": "IMF",
    "loss": "adaptive_hinge",
}


def bulk_fetch(
    start_date: datetime.date, end_date: datetime.date
) -> List[Dict[str, Any]]:
    raise NotImplementedError


NON_ARTICLE_PREFIXES = ["/author", "/wires"]


def extract_external_id(path: str) -> Optional[str]:
    """Request content ID from a url from ARC API

    :path:an Inquirer url
    :return contentID: Unique ID of url
    """
    params = {
        "website_url": path,
        "published": "true",
        "website": API_SITE,
        "included_fields": "_id",
    }

    for prefix in NON_ARTICLE_PREFIXES:
        if path.startswith(prefix):
            return None

    try:
        res = safe_get(API_URL, API_HEADER, params)
        res = res.json()
        contentID = res["_id"]
        return contentID
    except Exception as e:
        msg = f"Error fetching article url: {path}"
        logging.exception(msg)
        raise ArticleFetchError(msg) from e

    return None


def get_date(res_val: dict) -> str:
    """ARC response date parser. PI response includes timezone

    :res_val: JSON payload from ARC API
    :return: Isoformat date string
    """
    res_val = datetime.strptime(
        res_val["publish_date"], "%Y-%m-%dT%H:%M:%S.%fZ"
    ).isoformat()
    return res_val


def get_path(res_val: dict) -> str:
    """ARC response canonical path parser

    :res_val: JSON payload from ARC API
    :return: Canonical URL Path for external_ID
    E.g.,: /news/philadelphia-mayor-jim-kenney-johnny-doc-bobby-henon-convicted-20211116.html
    """
    return res_val["canonical_url"]


def get_headline(res_val: dict) -> str:
    """ARC response headline parser

    :res_val: JSON payload from ARC API
    :return: meta_title (if available) or basic title
    """
    res_val = res_val["headlines"]
    if "meta_title" in res_val:
        return res_val["meta_title"]

    return res_val["basic"]


def parse_article_metadata(page: Response, external_id: str) -> dict:
    """ARC API JSON parser

    :page: JSON Payload from ARC for an external_id
    :external_id: Unique identifier for URL
    :return: Relevant metadata from an API response
    """
    logging.info(f"Parsing metadata from id: {external_id}")

    metadata = {}
    parse_keys = [
        ("title", get_headline),
        ("published_at", get_date),  # example published_at: '2021-11-17T00:44:12.319Z'
        ("path", get_path),
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
    """ARC API response validator

    :res: ARC API JSON response payload
    :return: None if no errors; otherwise string describing validation issue
    """
    try:
        res = res.json()
    except Exception as e:
        return e

    if "headlines" not in res or (
        "headlines" in res and "basic" not in res["headlines"]
    ):
        return "Article missing headline"

    if "canonical_url" not in res:
        return "Article canonical URL missing"

    if "publish_date" not in res:
        return "Article publish date missing"

    try:
        datetime.strptime(res["publish_date"], "%Y-%m-%dT%H:%M:%S.%fZ").isoformat()
    except Exception as e:
        return e

    return None


def validate_article(external_id: str, path: str) -> (Response, str, Optional[str]):
    """ARC API validation handler

    :external_id: Unique identifier for a URL
    :return: JSON payload, external_id and an optional error message
    """
    params = {
        "_id": external_id,
        "website": API_SITE,
        "published": "true",
        "included_fields": "headlines,publish_date,_id,canonical_url",
    }

    logging.info(f"Validating article url: {external_id}")

    try:
        res = safe_get(API_URL, API_HEADER, params)
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


PI_SITE = Site(
    NAME,
    FIELDS,
    PARAMS,
    transform_data_google_tag_manager,
    extract_external_id,
    parse_article_metadata,
    validate_article,
    bulk_fetch,
)
