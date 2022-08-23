import logging
from typing import Any, Dict, List

import pandas as pd

from sites.site import Site


def validate(data_fetched: List[Dict[str, Any]], external_ids_to_fetch: List[str]) -> None:
    num_to_fetch = len(external_ids_to_fetch)
    num_fetched = len(data_fetched)

    if num_to_fetch != num_fetched:
        logging.warning(f"No. articles fetched ({num_fetched}) doesn't match no. articles to fetch ({num_to_fetch}).")


def run(site: Site, interactions_data: pd.DataFrame) -> pd.DataFrame:
    """
    Fetch text data of all articles included in the `interactions_data` table and join them with article ID metadata values.
    """
    # FETCH
    # Grab unique external IDs from interactions table
    # https://stackoverflow.com/questions/46839277/series-unique-vs-list-of-set-performance
    external_ids = list(set(interactions_data["external_id"]))

    # Fetch article text data using said IDs
    data_fetched: List[Dict[str, Any]] = site.bulk_fetch_by_article_id(external_ids)

    # Validation
    validate(data_fetched, external_ids)

    # PREPROCESS
    # Create text representation of each article
    data = [{"external_id": article["external_id"], "text": site.get_article_text(article)} for article in data_fetched]
    df_data = pd.DataFrame(data, dtype=str)

    # Extract article metadata (LNL DB article ID, categorial item ID, publish date) from interactions_data
    df_metadata = interactions_data[["article_id", "external_id", "published_at"]].dropna().drop_duplicates()

    # Merge article metadata with article text representations; the result is a DataFrame with 4 columns:
    # article_id, external_id, published_at and text
    df = df_metadata.merge(df_data, how="inner", on="external_id")

    return df
