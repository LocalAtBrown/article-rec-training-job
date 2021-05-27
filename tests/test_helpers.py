import numpy as np
import pandas as pd
import pytest

from datetime import datetime, timedelta
from scipy.sparse import csr_matrix

from job.helpers import get_similarities, get_orders, get_weights
from job.steps.implicit_mf import ImplicitMF
from db.mappings.base import tzaware_now


@pytest.fixture(scope="module")
def model():
    counts = np.array([[0, 0.25, 0.75], [0, 0.5, 0.75], [0.25, 0.5, 0.75]])
    model = ImplicitMF(
        counts=csr_matrix(counts), num_factors=1, num_iterations=10, reg_param=0.0
    )
    model.item_vectors = counts
    return model


@pytest.fixture(scope="module")
def article_df():
    articles = []
    articles.append(
        {"published_at": tzaware_now() - timedelta(days=1), "external_id": "1"}
    )
    articles.append(
        {"published_at": tzaware_now() - timedelta(days=10), "external_id": "2"}
    )
    articles.append(
        {"published_at": tzaware_now() - timedelta(days=50), "external_id": "3"}
    )
    articles.append(
        {"published_at": tzaware_now() - timedelta(days=100), "external_id": "4"}
    )
    article_df = pd.DataFrame(articles)
    return article_df


@pytest.fixture(scope="module")
def external_ids():
    return ["1", "2", "4"]


def _test_similarities(model):
    similarities = get_similarities(model)
    assert similarities.shape == (3, 3)
    assert all([similarities[i, i] > 0.999 for i in range(similarities.shape[0])])
    return similarities


def _test_weights(external_ids, article_df):
    regular_weights = get_weights(external_ids, article_df, half_life=float("inf"))
    assert regular_weights.shape == (3,)
    assert regular_weights[0] == regular_weights[1] == regular_weights[2]
    decayed_weights = get_weights(external_ids, article_df, half_life=10)
    assert decayed_weights.shape == (3,)
    assert decayed_weights[0] > decayed_weights[1] > decayed_weights[2]
    assert (0 < decayed_weights).all()
    assert (decayed_weights < 1).all()
    return decayed_weights


def _test_orders(similarities, weights):
    orders = get_orders(similarities, weights)
    assert orders.shape == (3, 3)
    assert (orders == np.array([[0, 1, 2], [1, 0, 2], [1, 0, 2]])).all()
    return orders


def test_article_recommendations(model, external_ids, article_df):
    similarities = _test_similarities(model)
    weights = _test_weights(external_ids, article_df)
    orders = _test_orders(similarities, weights)
