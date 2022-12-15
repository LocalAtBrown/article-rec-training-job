import logging
from typing import List, Tuple

import numpy as np
from scipy.spatial import distance

from db.mappings.recommendation import Rec
from job.helpers.itertools import batch
from lib.metrics import write_metric

DEFAULT_BATCH_SIZE = 2000
# this batch size means that each iteration consumes ~ 176MB


class KNN:
    def __init__(self, embeddings: np.ndarray, decays: np.ndarray, batch_size=DEFAULT_BATCH_SIZE):
        """KNN constructor with helpers to get the K nearest indices, decay embeddings
        KNN stores similarities on a [0,1] scale, where 1 is similar, 0 dissimilar
        This class calculates the similarities one row at a time, in order to save memory
        See KNN class for multi-dimensional vectorized implementation (faster, but less memory efficient)
        """
        self.embeddings = embeddings
        self.batch_size = batch_size
        self.decays = decays

    def _get_similar_for_indices(
        self,
        idxs: np.ndarray,
        n_recs: int,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        Calculate n_recs decayed similarities for articles given by index array idx
        Return array idx*n_recs of indexes for the nearest articles
        """
        # Calculate cosine distances
        # Format values on a [0,2] scale, where 0 is closer,
        # to values on a [0,1] scale, where 1 is closer.
        distances = distance.cdist(self.embeddings[idxs], self.embeddings, metric="cosine")
        if 0 in idxs:
            logging.info(
                f"Batch size {self.batch_size}, N={len(self.embeddings)}, mem usage: {distances.size * distances.itemsize}"
            )
            write_metric("distance_mem_size", distances.size * distances.itemsize)
        scaled_distances = np.nan_to_num((2 - distances) / 2, copy=False, neginf=0.0, posinf=1.0)

        # multiply every similarity by its corresponding decay weight.
        # reset the diagonal equal to 1.
        decayed_distances = self.decays * scaled_distances
        for i, j in enumerate(idxs):
            decayed_distances[i][j] = 1.0

        # drop the most similar observation, which is just itself
        # then reverse so that recommendations are sorted in descending order
        sorted_indices = decayed_distances.argsort()[:, ::-1][:, :n_recs:]
        sorted_scores = np.array([decayed_distances[i][v] for i, v in enumerate(sorted_indices)])
        return sorted_scores, sorted_indices

    def get_similar_indices(self, n_recs: int) -> Tuple[np.ndarray, np.ndarray]:
        """
        Get the nearest n_rec indices and corresponding weights
        for every article in the dataset
        """
        n = len(self.embeddings)
        scores = np.ndarray((n, n_recs)).astype(float)
        indices = np.ndarray((n, n_recs)).astype(int)

        for i in batch(range(n), self.batch_size):
            scores_i, indices_i = self._get_similar_for_indices(i, n_recs)
            scores[i] = scores_i
            indices[i] = indices_i
            logging.info(f"running knn batch {i} of {n}")

        return scores, indices


# TODO: This is lifted right out of job/helpers/collaborative_filter/train_model.py.
# When refactoring CF, have it uses this one instead.
def map_neighbors_to_recommendations(
    model_item_ids: np.ndarray,
    external_ids: np.ndarray,
    article_ids: np.ndarray,
    model_item_neighbor_indices: np.ndarray,
    similarities: np.ndarray,
) -> List[Rec]:
    """
    Create (source, target) recommendations from a list of articles and their nearest neighbors and corresponding similarity scores.

    Parameters
    ----------
    model_item_ids : np.ndarray
        Article IDs as used by the model
    external_ids : np.ndarray
        Article IDs native to site
    article_ids : np.ndarray
        Article IDs native to LNL DB
    model_item_neighbor_indices : np.ndarray
        2D array documenting each article's nearest neighbors using indices in the `article_ids` list
    similarities : np.ndarray
        2D array documenting similarity scores corresponding to entries in `model_item_neighbor_indices`
    ----------
    """
    recs = []

    for i in model_item_ids:
        item_source_external_id = external_ids[i]

        # Map the K nearest neighbors indexes to the map LNL DB article_id, also get the distances
        item_neighbors = article_ids[model_item_neighbor_indices[i][1:]]
        item_neighbor_scores = similarities[i][1:]

        item_recs = [
            Rec(source_entity_id=item_source_external_id, recommend_article_id=recommended_item_id, score=similarity)
            for (recommended_item_id, similarity) in zip(item_neighbors, item_neighbor_scores)
        ]
        recs.extend(item_recs)

    return recs
