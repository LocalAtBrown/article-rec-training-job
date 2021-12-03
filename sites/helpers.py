from typing import Dict
import requests as req

from retrying import retry


class ArticleScrapingError(Exception):
    pass


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def safe_get(url: str, headers: Dict[str, str] = None) -> str:
    TIMEOUT_SECONDS = 30
    default_headers = {"User-Agent": "article-rec-training-job/1.0.0"}
    if headers:
        default_headers.update(headers)
    page = req.get(url, timeout=TIMEOUT_SECONDS, headers=default_headers)
    return page
