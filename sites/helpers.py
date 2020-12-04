import requests as req

from retrying import retry


@retry(stop_max_attempt_number=5, wait_exponential_multiplier=1000)
def safe_get(url: str) -> str:
    TIMEOUT_SECONDS = 30
    page = req.get(url, timeout=TIMEOUT_SECONDS)
    return page
