import time
from typing import Optional, List, Dict, Any, Union
from requests.models import Response
from datetime import datetime
import logging

from lib.config import config
from sites.helpers import (
    GOOGLE_TAG_MANAGER_RAW_FIELDS,
    ScrapeFailure,
    ArticleScrapingError,
    transform_data_google_tag_manager,
    safe_get,
    ms_timestamp,
)
from sites.site import Site

"""
ARC API documentation
https://www.notion.so/a8698dd6527140aaba8acfc29be40aa8?v=d30e06f348e94063ab4f451a345bb0d2&p=209fa6fada864bc0a1555622bb559181
"""

POPULARITY_WINDOW = 1
DOMAIN = "www.inquirer.com"
NAME = "philadelphia-inquirer"
FIELDS = GOOGLE_TAG_MANAGER_RAW_FIELDS

API_URL = "https://api.pmn.arcpublishing.com/content/v4"
API_KEY = config.get("INQUIRER_TOKEN")
API_HEADER = {"Authorization": API_KEY}
API_SITE = "philly-media-network"
TRAINING_PARAMS = {
    "hl": 120,
    "embedding_dim": 256,
    "epochs": 2,
    "tune": False,
    "tune_params": ["epochs", "embedding_dim"],
    "tune_ranges": [[1, 3, 1], [30, 180, 50]],
    "model": "IMF",
    "loss": "adaptive_hinge",
}

SCRAPE_CONFIG = {
    "concurrent_requests": 2,
    "requests_per_second": 4,
}


def bulk_fetch(
    start_date: datetime.date, end_date: datetime.date
) -> List[Dict[str, Any]]:
    logging.info(f"Fetching articles from {start_date} to {end_date}")
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.min.time())
    start_ts = ms_timestamp(start_dt)
    end_ts = ms_timestamp(end_dt)
    params = {
        "q": f"publish_date:[{start_ts} TO {end_ts}]",
        "include_distributor_category": "staff",
        "_sourceInclude": "headlines,publish_date,_id,canonical_url",
        "website": API_SITE,
        "size": 100,  # inquirer publishes ~50 articles per day
    }
    res = safe_get(f"{API_URL}/search/published", API_HEADER, params, SCRAPE_CONFIG)
    json_res = res.json()
    metadata = [
        parse_article_metadata(a, a["_id"], a["canonical_url"])
        for a in json_res["content_elements"]
    ]
    return metadata


INVALID_PREFIXES = ["/author", "/wires", "/zzz-systest"]


def extract_external_id(path: str) -> Optional[str]:
    """Request content ID from a url from ARC API

    :path:an Inquirer url
    :return contentID: Unique ID of url
    """
    params = {
        "website_url": path,
        "published": "true",
        "website": API_SITE,
        "included_fields": "_id,source,taxonomy",
    }

    for prefix in INVALID_PREFIXES:
        if path.startswith(prefix):
            raise ArticleScrapingError(
                ScrapeFailure.FAILED_SITE_VALIDATION,
                path,
                external_id=None,
                msg="Skipping path with invalid prefix",
            )

    try:
        res = safe_get(API_URL, API_HEADER, params, SCRAPE_CONFIG)
        res = res.json()
    except Exception as e:
        raise ArticleScrapingError(
            ScrapeFailure.FETCH_ERROR, path, external_id=None
        ) from e

    if "_id" not in res:
        raise ArticleScrapingError(ScrapeFailure.NO_EXTERNAL_ID, path, external_id=None)

    external_id = res["_id"]

    IN_HOUSE_PLATFORMS = {"composer", "ellipsis"}
    if res.get("source", {}).get("system") not in IN_HOUSE_PLATFORMS:
        error_msg = "Not in-house article"
        raise ArticleScrapingError(
            ScrapeFailure.FAILED_SITE_VALIDATION, path, str(external_id), error_msg
        )

    TEST_SITE = "/zzz-systest"
    sites = res.get("taxonomy", {}).get("sites", [])
    if sites and sites[0].get("_id") == TEST_SITE:
        error_msg = "Test article"
        raise ArticleScrapingError(
            ScrapeFailure.FAILED_SITE_VALIDATION, path, str(external_id), error_msg
        )

    return external_id


def try_parsing_date(text: str, formats: List[str]) -> datetime:
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass
    raise ValueError("no valid date format found")


def get_date(res_val: dict) -> str:
    """ARC response date parser. PI response includes timezone

    :res_val: JSON payload from ARC API
    :return: Isoformat date string
    """
    formats = [
        "%Y-%m-%dT%H:%M:%SZ",  # 2021-12-01T15:53:20Z
        "%Y-%m-%dT%H:%M:%S.%fZ",  # 2021-11-17T00:44:12.319Z
    ]
    dt = try_parsing_date(res_val["publish_date"], formats)
    return dt.isoformat()


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

    return res_val.get("meta_title") or res_val["basic"]


def get_external_id(res_val: dict) -> str:
    external_id = res_val["_id"]
    return external_id


def parse_article_metadata(
    page: Union[Response, dict], external_id: str, path: str
) -> dict:
    """ARC API JSON parser

    :page: JSON Payload from ARC for an external_id
    :external_id: Unique identifier for URL
    :return: Relevant metadata from an API response
    """

    metadata = {}
    parse_keys = [
        ("title", get_headline),
        ("path", get_path),
        ("published_at", get_date),
        ("external_id", get_external_id),
    ]

    if type(page) is dict:
        res = page
    else:
        res = page.json()

    for prop, func in parse_keys:
        val = None
        try:
            val = func(res)
        except Exception as e:
            msg = f"Error parsing {prop}"
            raise ArticleScrapingError(
                ScrapeFailure.FETCH_ERROR, path, external_id, msg
            ) from e
        metadata[prop] = val

    return metadata


def validate_response(res: Response) -> Optional[str]:
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


def fetch_article(external_id: str, path: str) -> Response:
    """Fetch and validate article from the ARC API

    :external_id: Unique identifier for a URL
    :return: API response
    :throws: ArticleScrapingError
    """
    params = {
        "_id": external_id,
        "website": API_SITE,
        "published": "true",
        "included_fields": "headlines,publish_date,_id,canonical_url",
    }

    try:
        res = safe_get(API_URL, API_HEADER, params, SCRAPE_CONFIG)
    except Exception as e:
        msg = f"Error fetching article url: {API_URL}"
        raise ArticleScrapingError(
            ScrapeFailure.FETCH_ERROR, path, external_id, msg
        ) from e

    error_msg = validate_response(res)
    if error_msg:
        raise ArticleScrapingError(
            ScrapeFailure.MALFORMED_RESPONSE, path, external_id, error_msg
        )

    return res


PI_SITE = Site(
    NAME,
    FIELDS,
    TRAINING_PARAMS,
    SCRAPE_CONFIG,
    transform_data_google_tag_manager,
    extract_external_id,
    parse_article_metadata,
    fetch_article,
    bulk_fetch,
    POPULARITY_WINDOW,
)
