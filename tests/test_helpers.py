import numpy as np
import pandas as pd
import pytest

from datetime import datetime, timedelta
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

from db.mappings.base import tzaware_now
from job.steps.train_model import map_nearest
from job.steps.knn import KNN
from job.steps.trainer import Trainer
from job.steps.train_model import _spotlight_transform


@pytest.fixture(scope="module")
def user_ids():
    return np.array([3, 3, 2, 1, 3, 3, 1, 2])


@pytest.fixture(scope="module")
def external_ids():
    return np.array([3, 2, 2, 2, 0, 1, 0, 1])


@pytest.fixture(scope="module")
def durations():
    return np.array([2, 3, 0, 4, 3, 0, 3, 0])


@pytest.fixture(scope="module")
def session_dates():
    return np.array(
        [
            "12/13/2020",
            "12/13/2020",
            "12/14/2020",
            "12/13/2020",
            "12/14/2020",
            "12/15/2020",
            "12/14/2020",
            "12/15/2020",
        ]
    )


@pytest.fixture(scope="module")
def publish_dates():
    return np.array(
        [
            "12/14/2020",
            "12/13/2020",
            "12/13/2020",
            "12/13/2020",
            "12/11/2020",
            "12/12/2020",
            "12/11/2020",
            "12/12/2020",
        ]
    )


@pytest.fixture(scope="module")
def article_ids():
    return np.array([14, 13, 13, 13, 11, 12, 11, 12])


@pytest.fixture(scope="module")
def decays():
    return np.array([0.9, 1, 1, 1, 0.95, 0.98, 0.95, 0.98])


def _test_similarities(embeddings: np.ndarray, n_recs: int, decays: np.ndarray):
    """Checks that n recs are returned and the most similar rec is identical"""
    knn_index = KNN(embeddings, np.unique(decays))
    similarities, indices = knn_index.get_similar_indices(n_recs)
    assert similarities.shape == (4, n_recs)
    assert all([similarities[i, 0] == 1.0 for i in range(similarities.shape[0])])
    return similarities, indices


def _test_orders(
    n_recs: int,
    nearest_indices: np.ndarray,
    similarities: np.ndarray,
    article_ids: np.ndarray,
):
    """Gets the most similar recs for spotlight_id = 0, converted to article_ids (DB id format)
    The most similar recs should be 13 and 14.

    Here is why:
        Looking at spotlight_id = 0, we see two users implicity rated: user_id 1 and user_id 3.
        We also see that users 1 and 3 also consumed spotlight_id 2
        We also see that user_id 3 also consumed ids 1 and 3.
        However, we see that another user, 2, who consumed 2 and shared patterns with 1
        So because of their user consumption connections, spotlight_ids 2 and 3 are our recs
        2 and 3 share the most "implicit feedback" and are co-consumed the most.
        We map back to 13 and 14 (2->13, 3->14) because our spotlight ids match index-wise to the LNL db ids (article_ids)
        The map_nearest function maps the ids
    """
    _test_spotlight_id = 0
    rec_ids, rec_similarities = map_nearest(_test_spotlight_id, nearest_indices, similarities, article_ids)
    assert rec_ids.shape == (n_recs - 1,)
    assert (rec_ids == np.array([13, 14])).all()
    return rec_ids, rec_similarities


def test_article_recommendations(external_ids, user_ids, durations, session_dates, publish_dates, article_ids, decays):
    n_recs = 3
    warehouse_df = pd.DataFrame(
        {
            "client_id": user_ids,
            "external_id": external_ids,
            "article_id": article_ids,
            "duration": durations,
            "published_at": publish_dates,
            "session_date": session_dates,
        }
    )
    params = {"epochs": 35, "embedding_dim": 16, "model": "IMF"}
    _spotlight_transform(warehouse_df)
    model = Trainer(warehouse_df, datetime.now().date(), _spotlight_transform, params)
    model.fit()
    embeddings = model.model_embeddings
    distances, nearest_indices = _test_similarities(embeddings, n_recs, decays)
    _test_orders(n_recs, nearest_indices, distances, np.unique(article_ids))
