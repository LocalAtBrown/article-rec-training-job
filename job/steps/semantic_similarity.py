import logging
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

from lib.config import config
from sites.site import Site

# TODO: When adding functions to job.py in the future, maybe move config grab to job.py
# and pass pretrained model name to generate_embeddings() as a parameter?
PRETRAINED_MODEL_NAME = config.get("SS_ENCODER")


def fetch_data(site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Fetch data of all articles included in the interactions table.
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


def preprocess_data(site: Site, article_data: List[Dict[str]], interactions_data: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess fetched data into a DataFrame ready for training. Steps include:
    - Get a text representation for each article. The rule for creating such representations may differ between sites.
    - Merge fetched data with article metadata (LNL DB article ID, etc.)
    """
    # Create text representation of each article
    data = [{"external_id": article["external_id"], "text": site.get_article_text(article)} for article in article_data]
    df_data = pd.DataFrame(data, dtype=str)

    # Extract article metadata (LNL DB article ID, categorial item ID, publish date) from interactions_data
    df_metadata = interactions_data[["article_id", "external_id", "published_at"]].dropna().drop_duplicates()

    # Merge article metadata with article text representations; the result is a DataFrame with 4 columns:
    # article_id, external_id, published_at and text.
    # TODO: The schema for df will only include these 4 columns, which means we would benefit from type-enforcing
    # it, maybe with something like pandera (https://stackoverflow.com/questions/61386477/type-hints-for-a-pandas-dataframe-with-mixed-dtypes)?
    df = df_metadata.merge(df_data, how="inner", on="external_id")

    return df


def generate_embeddings(train_data: pd.DataFrame) -> np.ndarray:
    """
    Given article data DataFrame with a text column, create article-level text embeddings.
    """
    texts = train_data["text"].tolist()
    model = SentenceTransformer(PRETRAINED_MODEL_NAME, device="cpu")

    return model.encode(texts, convert_to_numpy=True)
