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
    return data
