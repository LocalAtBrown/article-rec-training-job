import logging
from typing import Any, Dict, List

from sites.site import Site


def run(site: Site, external_ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetches article data using site-specific ID-based bulk-fetching method, given a list of external IDs.
    """
    data = site.bulk_fetch_by_article_id(external_ids)

    num_to_fetch = len(external_ids)
    num_fetched = len(data)
    if num_to_fetch != num_fetched:
        logging.warning(f"No. articles fetched ({num_fetched}) doesn't match no. articles to fetch ({num_to_fetch}).")

    return data
