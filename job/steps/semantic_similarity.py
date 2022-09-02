import logging
from typing import Any, Dict, List

import pandas as pd

from sites.site import Site


def fetch_data(site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Main script. Fetch data of all articles included in the `interactions_data` table.
    """
    # Grab unique external IDs from Interactions table
    # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
    external_ids: List[str] = list(set(interactions_data["external_id"]))

    # Fetch article text data using said IDs
    data = site.bulk_fetch_by_external_id(external_ids)

    # Validation: check if all external IDs have been fetched
    num_to_fetch = len(external_ids)
    num_fetched = len(data)
    if num_to_fetch != num_fetched:
        logging.warning(f"No. articles fetched ({num_fetched}) doesn't match no. articles to fetch ({num_to_fetch}).")

    return data
