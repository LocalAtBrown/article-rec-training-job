from enum import Enum
from typing import List


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
