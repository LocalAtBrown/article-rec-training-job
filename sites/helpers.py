import requests as req

from retrying import retry


class ArticleScrapingError(Exception):
    pass


@retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
def safe_get(url: str, headers=None) -> str:
    TIMEOUT_SECONDS = 30
    page = req.get(url, timeout=TIMEOUT_SECONDS, headers=headers)
    return page
