import time
import logging
import numpy as np
import pandas as pd

from datetime import datetime, timezone, timedelta
from scipy.spatial import distance
from typing import List

from db.helpers import create_rec
from job import preprocessors
from job.models import ImplicitMF
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")


def create_article_to_article_recs(
    model: ImplicitMF, model_id: int, external_ids: List[str], article_df: pd.DataFrame
):
    start_ts = time.time()
    created_recs = 0
    vector_similarities = get_similarities(model)
    vector_weights = get_weights(external_ids, article_df)
    vector_orders = get_orders(vector_similarities, vector_weights)

    for source_index, ranked_recommendation_indices in enumerate(vector_orders):
        source_external_id = external_ids[source_index]

        rec_ids = set()
        for recommendation_index in ranked_recommendation_indices[: MAX_RECS + 1]:
            if len(rec_ids) == MAX_RECS:
                break

            recommended_external_id = external_ids[recommendation_index]
            # If it's the article itself, skip it
            if recommended_external_id == source_external_id:
                continue

            matching_articles = article_df[
                article_df["external_id"] == recommended_external_id
            ]
            recommended_article_id = matching_articles["article_id"][0]
            # for distance, smaller values are more highly correlated
            # for score, higher values are more highly correlated
            score = vector_similarities[source_index][recommendation_index]
            # fix case when some scores are negative due to a rounding error
            score = max(score, 0.0)

            rec_id = create_rec(
                source_entity_id=int(source_external_id),
                model_id=model_id,
                recommended_article_id=recommended_article_id,
                score=score,
            )
            rec_ids.add(rec_id)
            created_recs += 1

    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", created_recs)


def get_similarities(model: ImplicitMF) -> np.array:
    vector_distances = distance.cdist(
        model.item_vectors, model.item_vectors, metric="cosine"
    )
    vector_similarities = 1 - vector_distances
    return vector_similarities


def get_weights(
    external_ids: List[str], article_df: pd.DataFrame, half_life: float = 10
) -> np.array:
    weights = np.ones(len(external_ids))
    publish_time_df = (
        article_df[["external_id", "published_at"]]
        .drop_duplicates("external_id")
        .set_index("external_id")
        .loc[external_ids]
    )
    publish_time_df["published_at"] = pd.to_datetime(publish_time_df.published_at)
    date_delta = (
        datetime.now(timezone.utc) - publish_time_df.published_at
    ).dt.total_seconds() / (3600 * 60 * 24)
    return preprocessors.apply_decay(weights, date_delta, half_life)


def get_orders(similarities: np.array, weights: np.array):
    similarities *= weights
    orders = similarities.argsort()[:, ::-1]
    return orders
