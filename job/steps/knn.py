import logging

import numpy as np
from scipy.spatial import distance


class KNN:
    def __init__(self, embeddings: np.ndarray, decays: np.ndarray):
        """KNN constructor with helpers to get the K nearest indices, decay embeddings
        KNN stores similarities on a [0,1] scale, where 1 is similar, 0 dissimilar
        """
        self.similarities = self._get_cosines(embeddings)
        self._decay_embeddings(decays)
        logging.info("Embedding similarity matrix construted")

    def _get_cosines(self, embeddings: np.ndarray) -> np.ndarray:
        """Format values on a [0,2] scale, where 0 is closer,
        to values on a [0,1] scale, where 1 is closer.
        """
        distances = distance.cdist(embeddings, embeddings, metric="cosine")
        return (2 - distances) / 2

    def _decay_embeddings(self, decays: np.ndarray) -> None:
        """multiply every column by its corresponding decay weight. set the diagonal equal to 1. every row's index corresponds to that recommendation."""
        embedding_matrix = np.nan_to_num(
            self.similarities, copy=False, neginf=0.0, posinf=1.0
        )
        decayed_matrix = embedding_matrix * decays.reshape((1, decays.size))
        np.fill_diagonal(decayed_matrix, 1.0)
        self.similarities = decayed_matrix

    def get_similar_indices(self, n_recs: int) -> (np.ndarray, np.ndarray):
        """Get the nearest n_rec indices; get the corresponding weights"""
        sorted_recs = self.similarities.argsort()[:, ::-1][:, :n_recs]
        return (
            np.array([self.similarities[i][v] for i, v in enumerate(sorted_recs)]),
            sorted_recs,
        )
