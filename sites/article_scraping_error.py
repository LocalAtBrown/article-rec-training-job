from enum import Enum


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

    pass
