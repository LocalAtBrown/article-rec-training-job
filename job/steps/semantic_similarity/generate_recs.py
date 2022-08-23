import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize

from job.helpers.knn import KNN


def run(embeddings: np.ndarray, article_data: pd.DataFrame, max_recs: int) -> np.ndarray:
    embeddings_normalized = normalize(embeddings, axis=1, norm="l2")
    # Not doing time decay on SS for the time being
    decays = np.ones(embeddings.shape[0])
    searcher = KNN(embeddings_normalized, decays)
    # max_recs + 1 because an article's top match is always itself
    similarities, nearest_indices = searcher.get_similar_indices(max_recs + 1)

    import pdb

    pdb.set_trace()
