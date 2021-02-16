import numpy as np
import pandas as pd
import pytest
import unittest

from datetime import datetime, timedelta
from scipy.sparse import csr_matrix

from job import helpers
from job.helpers import get_similarities, get_orders, get_weights
from job.models import ImplicitMF


@pytest.fixture(scope='module')
def model():
    counts = np.array([[0, 0.25, 0.75], [0, 0.5, 0.75], [0.25, 0.5, 0.75]])
    model = ImplicitMF(counts=csr_matrix(counts), num_factors=1, num_iterations=10, reg_param=0.0)
    model.item_vectors = counts
    return model


@pytest.fixture(scope='module')
def article_df():
    articles = []
    articles.append({'published_at': datetime.now() - timedelta(days=1), 'article_id': '1'})
    articles.append({'published_at': datetime.now() - timedelta(days=10), 'article_id': '2'})
    articles.append({'published_at': datetime.now() - timedelta(days=50), 'article_id': '3'})
    articles.append({'published_at': datetime.now() - timedelta(days=100), 'article_id': '4'})
    article_df = pd.DataFrame(articles)
    return article_df


@pytest.fixture(scope='module')
def external_ids():
    return ['1', '2', '4']


def generate_row(client_id, external_id):
    return {
        "client_id": client_id,
        "external_id": external_id,
        "event_category": "pageview",
        "event_action": "pageview",
        "session_date": datetime.now(),
        "activity_time": datetime.now(),
    }


class TestHelpers(unittest.TestCase):
    def test_calculate_default_recs(self) -> None:
        basit = "client.a"
        kai = "client.b"
        article_a = 123
        article_b = 456

        data = [
            # basit and kai both read article a once
            generate_row(basit, article_a),
            generate_row(kai, article_a),
            # while basit was on a date with jonathan, kai read article b three times
            generate_row(kai, article_b),
            generate_row(kai, article_b),
            generate_row(kai, article_b),
        ]

        df = pd.DataFrame(data)
        top_pageviews = helpers.calculate_default_recs(df)
        # expect article a to be ranked higher than article b
        assert all(top_pageviews.index == [article_a, article_b])
        # expect article a to have two unique pageviews, and article b to have one
        assert all(top_pageviews == [2, 1])


def _test_similarities(model):
    similarities = get_similarities(model)
    assert similarities.shape == (3, 3)
    assert all([similarities[i,i] > 0.999 for i in range(similarities.shape[0])])
    return similarities


def _test_weights(external_ids, article_df):
    regular_weights = get_weights(external_ids, article_df, publish_time_decay=False)
    assert regular_weights.shape == (3,)
    assert regular_weights[0] == regular_weights[1] == regular_weights[2]
    decayed_weights = get_weights(external_ids, article_df, publish_time_decay=True)
    assert decayed_weights.shape == (3,)
    assert decayed_weights[0] > decayed_weights[1] > decayed_weights[2]
    assert (0 < decayed_weights).all()
    assert (decayed_weights < 1).all()
    return decayed_weights

def _test_orders(similarities, weights):
    orders = get_orders(similarities, weights)
    assert orders.shape == (3, 3)
    assert (orders == np.array([[0, 1, 2], [0, 1, 2], [0, 1, 2]])).all()
    return orders


def test_article_recommendations(model, external_ids, article_df):
    similarities = _test_similarities(model)
    weights = _test_weights(external_ids, article_df)
    orders = _test_orders(similarities, weights)
