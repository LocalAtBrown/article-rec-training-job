from typing import List

import numpy as np
from sklearn.preprocessing import normalize

from job.helpers.knn import KNN
from lib.config import config

MAX_RECS = config.get("MAX_RECS")


def run(embeddings: np.ndarray, external_ids: List[str]) -> np.ndarray:
    embeddings_normalized = normalize(embeddings, axis=1, norm="l2")
    # Not doing time decay on SS for the time being
    decays = np.ones(embeddings.shape[0])
    searcher = KNN(embeddings_normalized, decays)
    similarities, nearest_indices = searcher.get_similar_indices(MAX_RECS)
