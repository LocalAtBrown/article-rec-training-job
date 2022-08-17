import datetime
import logging
import time
from typing import Any, List, Tuple

import numpy as np
import pandas as pd

from db.mappings.recommendation import Rec
from job.steps.knn import KNN
from job.steps.trainer import Trainer
from lib.config import config

MAX_RECS = config.get("MAX_RECS")


def _spotlight_transform(prepared_df: pd.DataFrame, batch_size: int, random_seed: int, **kwargs: Any) -> pd.DataFrame:
    """Transform data for Spotlight
    :prepared_df: Dataframe with user-article interactions
    :return: (prepared_df)
    """
    prepared_df = prepared_df.dropna()

    # If DataFrame length divides batch_size with a remainder of 1, Spotlight's BilinearNet inside IMF will
    # throw an IndexError (see https://github.com/maciejkula/spotlight/issues/107) that in the past was
    # responsible for a few failed job runs (see https://github.com/LocalAtBrown/article-rec-training-job/pull/140).
    #
    # Until this Spotlight bug is fixed, as a short-term measure, we randomly remove 1 entry from the
    # DataFrame corresponding to the article with the highest view count so as to minimally affect performance.
    num_interactions = prepared_df.shape[0]
    if num_interactions % batch_size == 1:
        # External ID of most read article
        id_article_most_read = prepared_df[["external_id"]].groupby("external_id").size().idxmax()
        # Randomly chooses an index among interactions involving most interacted article
        index_to_drop = np.random.default_rng(random_seed).choice(
            prepared_df[prepared_df["external_id"] == id_article_most_read].index, size=1, replace=False, shuffle=False
        )
        # Drop in-place
        prepared_df = prepared_df.drop(index=index_to_drop)
        logging.warning(
            f"Found {num_interactions} reader-article interactions, which leaves a remainder of 1 when divided by a batch size of {batch_size} and would trigger a Spotlight bug. "
            + f"To prevent this, 1 random interaction corresponding to external ID {id_article_most_read} has been dropped."
        )

    prepared_df["published_at"] = pd.to_datetime(prepared_df["published_at"])
    prepared_df["session_date"] = pd.to_datetime(prepared_df["session_date"])
    prepared_df["session_date"] = prepared_df["session_date"].dt.date
    prepared_df["external_id"] = prepared_df["external_id"].astype("category")
    prepared_df["item_id"] = prepared_df["external_id"].cat.codes
    prepared_df["user_id"] = prepared_df["client_id"].factorize()[0]
    prepared_df["timestamp"] = prepared_df["session_date"].factorize()[0] + 1

    return prepared_df


def train_model(X: pd.DataFrame, params: dict, experiment_time: datetime.datetime) -> Tuple[np.ndarray, pd.DataFrame]:
    """Train spotlight model

    X: pandas dataframe of interactions
    params: Hyperparameters
    experiment_time: time to benchmark decay against

    return: (embeddings: Spotlight model embeddings for each unique article,
            dates_df: pd.DataFrame with columns:
                external_item_ids: Publisher unique article IDs,
                internal_ids: Spotlight article IDs,
                article_ids: LNL DB article IDs,
                date_decays: Decay factors for each article [0,1]
            )
    """
    model = Trainer(X, experiment_time, _spotlight_transform, params)
    model.fit()
    return model.model_embeddings, model.model_dates_df


def map_nearest(
    spotlight_id: int,
    nearest_indices: np.ndarray,
    distances: np.ndarray,
    article_ids: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances"""
    return (article_ids[nearest_indices[spotlight_id][1:]], distances[spotlight_id][1:])


def get_recommendations(X: pd.DataFrame, params: dict, dt: datetime.datetime) -> List[Rec]:
    logging.info("Starting model training...")
    embeddings, df = train_model(X, params, dt)

    start_ts = time.time()

    logging.info("Calcuating KNN...")
    # Use KNN similarity to calculate score of each recommendation
    knn_index = KNN(embeddings, df["date_decays"].values)
    similarities, nearest_indices = knn_index.get_similar_indices(MAX_RECS + 1)

    knn_latency = time.time() - start_ts
    logging.info(f"Total latency to find K-Nearest Neighbors: {knn_latency}")

    spotlight_ids = df["item_id"].values
    external_item_ids = df["external_id"].values
    article_ids = df["article_id"].values
    recs = []

    for i in spotlight_ids:
        source_external_id = external_item_ids[i]
        recommendations = map_nearest(i, nearest_indices, similarities, article_ids)

        recs += [
            Rec(
                source_entity_id=source_external_id,
                recommended_article_id=recommended_item_id,
                score=similarity,
            )
            for (recommended_item_id, similarity) in zip(*recommendations)
        ]
    return recs
