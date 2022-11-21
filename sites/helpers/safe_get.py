import time
from typing import Dict, Optional

import requests as req
from retrying import retry


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
