import logging
import time
from enum import Enum
from typing import Callable, Dict, List, Optional

import requests as req
from requests.exceptions import HTTPError
from requests.models import Response
from retrying import retry

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

    # Many times, the request hits a 4xx or 5xx, but no exception is raised
    # This makes sure an exception is raised and allows the retry decorator to work.
    #
    # Some notes:
    # - Since we already have exponential backoff, no need to treat 429 error any differently.
    # - During the last retry attempt, if this still raises an exception, that exception's already handled
    # by a try-except block in each site's fetch_article method.
    page.raise_for_status()

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


# TODO: Once merging SS, combine this with SS ArticleBatchScrapingError
class ArticleBulkScrapingError(Exception):
    def __init__(self, errorType: ScrapeFailure, msg: str):
        self.errorType = errorType
        self.msg = msg