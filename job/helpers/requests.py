from typing import TypedDict


class ScrapeConfig(TypedDict):
    """
    Scraping config.
    """

    concurrent_requests: int
    requests_per_second: int
