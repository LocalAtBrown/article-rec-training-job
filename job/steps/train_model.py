import datetime
import logging
import time
from typing import List, Tuple

import pandas as pd
import numpy as np

from db.mappings.recommendation import Rec
from job.steps.knn import KNN
from job.steps.trainer import Trainer
from lib.config import config

MAX_RECS = config.get("MAX_RECS")


def _spotlight_transform(prepared_df: pd.DataFrame) -> pd.DataFrame:
    """Transform data for Spotlight
    :prepared_df: Dataframe with user-article interactions
    :return: (prepared_df)
    """
    prepared_df = prepared_df.dropna()
    prepared_df["published_at"] = pd.to_datetime(prepared_df["published_at"])
    prepared_df["session_date"] = pd.to_datetime(prepared_df["session_date"])
    prepared_df["session_date"] = prepared_df["session_date"].dt.date
    prepared_df["external_id"] = prepared_df["external_id"].astype("category")
    prepared_df["item_id"] = prepared_df["external_id"].cat.codes
    prepared_df["user_id"] = prepared_df["client_id"].factorize()[0]
    prepared_df["timestamp"] = prepared_df["session_date"].factorize()[0] + 1

    return prepared_df


def train_model(
    X: pd.DataFrame, params: dict, experiment_time: datetime.datetime
) -> Tuple[Trainer, pd.DataFrame]:
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
    return (model.model_embeddings, model.model_dates_df)


def map_nearest(
    spotlight_id: int,
    nearest_indices: np.ndarray,
    distances: np.ndarray,
    article_ids: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances"""
    return (article_ids[nearest_indices[spotlight_id][1:]], distances[spotlight_id][1:])


def get_recommendations(
    X: pd.DataFrame, params: dict, dt: datetime.datetime
) -> List[Rec]:
    embeddings, df = train_model(X, params, dt)

    start_ts = time.time()

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
