import time
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

import pandas as pd
import requests as req
from requests.models import Response
from retrying import retry

GOOGLE_TAG_MANAGER_RAW_FIELDS = {
    "collector_tstamp",
    "domain_userid",
    "event_name",
    "page_urlpath",
}

# Custom types
ResponseValidator = Callable[[Response], Optional[str]]


def ms_timestamp(dt: datetime) -> float:
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0


def transform_data_google_tag_manager(df: pd.DataFrame) -> pd.DataFrame:
    """
        requires a dataframe with the following fields:
            - domain_userid
            - collector_tstamp
            - page_urlpath
            - event_name
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
    transformed_df["activity_time"] = pd.to_datetime(df.collector_tstamp).dt.round("1s")
    transformed_df["session_date"] = pd.to_datetime(transformed_df.activity_time.dt.date)
    transformed_df["landing_page_path"] = df.page_urlpath
    transformed_df["event_name"] = df.event_name
    transformed_df["event_name"] = transformed_df["event_name"].astype("category")

    return transformed_df


class ScrapeFailure(Enum):
    UNKNOWN = "unknown"
    FETCH_ERROR = "fetch_error"
    NO_EXTERNAL_ID = "no_external_id"
    MALFORMED_RESPONSE = "malformed_api_response"
    FAILED_SITE_VALIDATION = "failed_site_validation"
    NO_PUBLISH_DATE = "no_publish_date"
    DUPLICATE_PATH = "duplicate_path"


class ArticleScrapingError(Exception):
    def __init__(self, errorType: ScrapeFailure, path: str, external_id, msg=""):
        self.error_type = errorType
        self.path = path
        self.msg = msg
        self.external_id = external_id


# TODO: Once merging SS, combine this with SS ArticleBatchScrapingError
class ArticleBulkScrapingError(Exception):
    def __init__(self, errorType: ScrapeFailure, msg: str):
        self.errorType = errorType
        self.msg = msg


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def safe_get(
    url: str,
    headers: Dict[str, str] = None,
    params: Optional[Dict] = None,
    scrape_config={},
) -> req.Response:
    TIMEOUT_SECONDS = 30
    default_headers = {"User-Agent": "article-rec-training-job/1.0.0"}
    if headers:
        default_headers.update(headers)
    page = req.get(url, timeout=TIMEOUT_SECONDS, params=params, headers=default_headers)

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
