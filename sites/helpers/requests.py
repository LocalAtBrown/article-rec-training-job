import time
from typing import Dict, Callable, Optional

import requests as req
from retrying import retry
from requests.exceptions import HTTPError
import logging
from typing import List
from requests.models import Response
from enum import Enum

# Custom types
ResponseValidator = Callable[[Response], Optional[str]]


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def safe_get(
    url: str,
    headers: Dict[str, str] = None,
    params: Optional[Dict] = None,
    scrape_config=None,
) -> req.Response:
    if scrape_config is None:
        scrape_config = {}
    timeout_seconds = 30
    default_headers = {"User-Agent": "article-rec-training-job/1.0.0"}
    if headers:
        default_headers.update(headers)
    page = req.get(url, timeout=timeout_seconds, params=params, headers=default_headers)
    if scrape_config.get("requests_per_second"):
        time.sleep(1 / scrape_config["requests_per_second"])
    return page


def validate_response(page: Response, validate_funcs: List[ResponseValidator]) -> Optional[str]:
    # Go through validation functions one by one, stop as soon as a message gets returned
    for func in validate_funcs:
        error_msg = func(page)
        if error_msg is not None:
            return error_msg
    return None


def validate_status_code(page: Response) -> Optional[str]:
    # Would be curious to see non-200 responses that still go through
    if page.status_code != 200:
        logging.info(f"Requested with resp. status {page.status_code}: {page.url}")
    try:
        # Raise HTTPError if error code is 400 or more
        page.raise_for_status()
        return None
    except HTTPError as e:
        return f'Request failed with error code {page.status_code} and message "{e}"'


class ScrapeFailure(Enum):
    UNKNOWN = "unknown"
    FETCH_ERROR = "fetch_error"
    NO_EXTERNAL_ID = "no_external_id"
    MALFORMED_RESPONSE = "malformed_api_response"
    FAILED_SITE_VALIDATION = "failed_site_validation"
    NO_PUBLISH_DATE = "no_publish_date"
    DUPLICATE_PATH = "duplicate_path"


class ArticleScrapingError(Exception):
    def __init__(self, error_type: ScrapeFailure, path: str, external_id, msg=""):
        self.error_type = error_type
        self.path = path
        self.msg = msg
        self.external_id = external_id

    pass


class ArticleBatchScrapingError(Exception):
    def __init__(self, external_ids: List[str], url: str, msg: str = "") -> None:
        self.url = url
        self.msg = msg
        self.external_ids = external_ids

    pass
