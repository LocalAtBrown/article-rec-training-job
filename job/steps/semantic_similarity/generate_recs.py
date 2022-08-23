from typing import List, Tuple

import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize

from db.mappings.recommendation import Rec
from job.helpers.knn import KNN


def map_nearest(
    model_index: int,
    nearest_indices: np.ndarray,
    distances: np.ndarray,
    article_ids: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances"""
    return (article_ids[nearest_indices[model_index][1:]], distances[model_index][1:])


def run(embeddings: np.ndarray, article_data: pd.DataFrame, max_recs: int) -> List[Rec]:
    embeddings_normalized = normalize(embeddings, axis=1, norm="l2")
    # Not doing time decay on SS for the time being
    decays = np.ones(embeddings.shape[0])
    searcher = KNN(embeddings_normalized, decays)
    # max_recs + 1 because an article's top match is always itself
    similarities, nearest_indices = searcher.get_similar_indices(max_recs + 1)

    # Create recs from similarities and nearest_indices
    # TODO: The following code, along with the map_nearest() function above, were lifted right out of
    # job/steps/collaborative_filtering/train_model.py. If we end up putting SS into production, refactoring
    # should involve getting rid of this redundancy.
    model_item_indices = article_data["external_id"].factorize()[0]
    external_item_ids = article_data["external_id"].values
    article_ids = article_data["article_id"].values
    recs = []

    for i in model_item_indices:
        source_external_id = external_item_ids[i]
        recommendations = map_nearest(i, nearest_indices, similarities, article_ids)

        item_recs = [
            Rec(source_entity_id=source_external_id, recommend_article_id=recommended_item_id, score=similarity)
            for (recommended_item_id, similarity) in zip(*recommendations)
        ]
        recs.extend(item_recs)

    return recs
