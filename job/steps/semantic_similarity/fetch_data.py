import logging
from typing import Any, Dict, List

import pandas as pd

from sites.site import Site


def get_external_ids(interactions_data: pd.DataFrame) -> List[str]:
    # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
    return list(set(interactions_data["external_id"]))


def validate(data_fetched: List[Dict[str, Any]], external_ids_to_fetch: List[str]) -> None:
    num_to_fetch = len(external_ids_to_fetch)
    num_fetched = len(data_fetched)

    if num_to_fetch != num_fetched:
        logging.warning(f"No. articles fetched ({num_fetched}) doesn't match no. articles to fetch ({num_to_fetch}).")


def run(site: Site, interactions_data: pd.DataFrame) -> pd.DataFrame:
    """
    Main script. Fetch data of all articles included in the `interactions_data` table.
    """
    # Grab unique external IDs from interactions table
    external_ids = get_external_ids(interactions_data)

    # Fetch article data using said IDs
    data: List[Dict[str, Any]] = site.bulk_fetch_by_article_id(external_ids)

    # Validation
    validate(data, external_ids)

    return data
