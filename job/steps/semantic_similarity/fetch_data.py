from typing import Any, Dict, List

import pandas as pd

from sites.site import Site

# from typing import Set


def run(site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Main script. Fetch data of all articles included in the `interactions_data` table.
    """
    # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
    # external_ids: Set[str] = set(interactions_data["external_id"])
    # TODO: Remove next line and uncomment preceding line as soon as bulk_fetch_by_external_id() is rewritten following Liam's suggestions
    external_ids = {"40933", "40929", "15", "2"}

    data = site.bulk_fetch_by_article_id(external_ids)
    return data
