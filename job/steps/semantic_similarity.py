import logging
import time
from typing import Any, Dict, List

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.preprocessing import normalize

from db.helpers import create_model, refresh_db, set_current_model
from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.helpers.itertools import batch
from job.helpers.knn import KNN, map_neighbors_to_recommendations
from lib.config import config
from sites.templates.site import Site

# TODO: When adding functions to job.py in the future, maybe move these job.py and pass to respective functions as a parameter?
PRETRAINED_MODEL_NAME = config.get("SS_ENCODER")
MAX_RECS = config.get("MAX_RECS")


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


def generate_recommendations(train_embeddings: np.ndarray, train_data: pd.DataFrame) -> List[Rec]:
    """
    Run article-level embeddings through a KNN and create recs from resulting neighbors.
    """
    embeddings_normalized = normalize(train_embeddings, axis=1, norm="l2")
    # TODO: Not doing time decay on SS for time being
    decays = np.ones(embeddings_normalized.shape[0])
    searcher = KNN(embeddings_normalized, decays)

    logging.info("Calculating KNN...")
    start_ts = time.time()

    # MAX_RECS + 1 because an article's top match is always itself
    similarities, nearest_indices = searcher.get_similar_indices(MAX_RECS + 1)

    logging.info(f"Total latency to find K-Nearest Neighbors: {time.time() - start_ts}")

    return map_neighbors_to_recommendations(
        model_item_ids=train_data["external_id"].factorize()[0],
        external_ids=train_data["external_id"].values,
        article_ids=train_data["article_id"].values,
        model_item_neighbor_indices=nearest_indices,
        similarities=similarities,
    )


@refresh_db
def save_recommendations(site: Site, recs: List[Rec], model_type: ModelType) -> None:
    """
    Save generated recommendations to database.
    """
    start_ts = time.time()
    # Create new model object in DB
    model_id = create_model(type=model_type.value, site=site.name)
    logging.info(f"Created model with id {model_id}")
    for rec in recs:
        rec.model_id = model_id

    logging.info(f"Writing {len(recs)} recommendations...")
    # Insert a small delay to avoid overwhelming the DB
    for rec_batch in batch(recs, n=50):
        Rec.bulk_create(rec_batch)
        time.sleep(0.05)

    logging.info(f"Updaing model objects in DB")
    set_current_model(model_id, model_type, site.name)

    latency = time.time() - start_ts
    # TODO: Unlike save_predictions in CF, not writing metrics to CloudWatch for now
    logging.info(f"rec_creation_time: {latency}s")
    logging.info(f"recs_creation_total: {len(recs)}")
