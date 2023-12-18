from dataclasses import dataclass


@dataclass
class Metrics:
    """
    Reporting metrics a page fetcher must provide.
    """

    num_urls_received: int
    num_pages_fetched: int
    num_articles_fetched: int
    time_taken_to_fetch_pages: float
