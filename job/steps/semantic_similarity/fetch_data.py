import logging
from typing import Any, Dict, List

import pandas as pd

from sites.site import Site


def run(site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Main script. Fetch data of all articles included in the `interactions_data` table.
    """
    # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
    external_ids: List[str] = list(set(interactions_data["external_id"]))

    data = site.bulk_fetch_by_article_id(external_ids)
    num_to_fetch = len(external_ids)
    num_fetched = len(data)

    if num_to_fetch != num_fetched:
        logging.warning(f"No. articles fetched ({num_fetched}) doesn't match no. articles to fetch ({num_to_fetch}).")

    return data
