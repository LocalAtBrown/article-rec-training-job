import numpy as np
import pandas as pd

from datetime import datetime, timezone

from lib.bucket import save_outputs
from scipy.spatial import distance
from typing import List

from job.steps.implicit_mf import ImplicitMF


@save_outputs("vector_similarities.npy")
def get_similarities(model: ImplicitMF) -> np.array:
    vector_distances = distance.cdist(
        model.item_vectors, model.item_vectors, metric="cosine"
    )
    vector_similarities = 1 - vector_distances
    return np.nan_to_num(vector_similarities)


@save_outputs("vector_weights.npy")
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
    return np.array(apply_decay(weights, date_delta, half_life))


def apply_decay(values: np.array, date_delta: int, half_life: float) -> np.array:
    """
    Computes exponential decay of value over date_delta, with a half life of half_life.
    Can be used for cumulative row-wise sums, by the principle that:

        exp(T3 - T1) = exp(T3 - T2) * exp(T2 - T1)

    :param values: (NumPy array of floats) values being decayed
    :param date_delta: (int) time span in days over which decay occurs
    :param half_life: (float) half life of decay
    :return: (NumPy array of floats) decayed values
    """
    # Decay factor should be ln(2) / lambda, where lambda is the desired half-life in days
    decay_constant = np.log(2) / half_life
    decayed_values = values * np.exp(-date_delta * decay_constant)
    return decayed_values


@save_outputs("vector_orders.npy")
def get_orders(similarities: np.array, weights: np.array):
    similarities *= weights
    orders = similarities.argsort()[:, ::-1]
    return orders



