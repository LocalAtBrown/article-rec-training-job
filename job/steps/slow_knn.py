import numpy as np
from scipy.spatial import distance


class SlowKNN:
    def __init__(self, embeddings: np.ndarray, decays: np.ndarray):
        """KNN constructor with helpers to get the K nearest indices, decay embeddings
        KNN stores similarities on a [0,1] scale, where 1 is similar, 0 dissimilar
        This class calculates the similarities one row at a time, in order to save memory
        See KNN class for multi-dimensional vectorized implementation (faster, but less memory efficient)
        """
        self.embeddings = embeddings
        self.decays = decays

    def _get_similar_for_index(
        self,
        idx: int,
        n_recs: int,
    ) -> (np.array, np.array):
        """
        Calculate n_recs decayed similarities for article given by index idx
        Return array of length n_recs of indexes for the nearest articles
        """
        # Calculate cosine distances
        # Format values on a [0,2] scale, where 0 is closer,
        # to values on a [0,1] scale, where 1 is closer.
        distances = distance.cdist(
            [self.embeddings[idx]], self.embeddings, metric="cosine"
        )[0]
        scaled_distances = np.nan_to_num(
            (2 - distances) / 2, copy=False, neginf=0.0, posinf=1.0
        )

        # multiply every similarity by its corresponding decay weight.
        # reset the diagonal equal to 1.
        decayed_distances = self.decays * scaled_distances
        decayed_distances[idx] = 1.0

        # drop the most similar observation, which is just itself
        # then reverse so that recommendations are sorted in descending order
        sorted_indices = decayed_distances.argsort()[-n_recs:][::-1]
        sorted_scores = decayed_distances[sorted_indices]
        return sorted_scores, sorted_indices

    def get_similar_indices(self, n_recs: int) -> (np.ndarray, np.ndarray):
        """
        Get the nearest n_rec indices and corresponding weights
        for every article in the dataset
        """
        scores = np.ndarray((len(self.embeddings), n_recs))
        indices = np.ndarray((len(self.embeddings), n_recs)).astype(int)

        for i in range(0, len(self.embeddings)):
            scores_i, indices_i = self._get_similar_for_index(i, n_recs)
            scores[i] = scores_i
            indices[i] = indices_i

        return scores, indices
