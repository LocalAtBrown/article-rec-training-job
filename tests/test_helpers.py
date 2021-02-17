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
    articles.append({'published_at': datetime.now() - timedelta(days=1), 'external_id': '1'})
    articles.append({'published_at': datetime.now() - timedelta(days=10), 'external_id': '2'})
    articles.append({'published_at': datetime.now() - timedelta(days=50), 'external_id': '3'})
    articles.append({'published_at': datetime.now() - timedelta(days=100), 'external_id': '4'})
    article_df = pd.DataFrame(articles)
    return article_df


@pytest.fixture(scope='module')
def external_ids():
    return ['1', '2', '4']


def generate_row(client_id, external_id, delta_secs=0):
    return {
        "client_id": client_id,
        "external_id": external_id,
        "event_category": "snowplow_amp_page_ping",
        "event_action": "impression",
        "session_date": datetime.now().date(),
        "activity_time": datetime.now() + timedelta(seconds=delta_secs),
    }


class TestHelpers(unittest.TestCase):
    def test_calculate_default_recs(self) -> None:
        basit = "client.a"
        kai = "client.b"
        article_a = 123
        article_b = 456

        data = [
            # basit and kai both read article a for 2 minutes
            generate_row(basit, article_a),
            generate_row(basit, article_a, 120),
            generate_row(kai, article_a),
            generate_row(kai, article_a, 120),
            # kai read article b for 1 minute, basit read article b for 30 seconds
            generate_row(kai, article_b, 120),
            generate_row(kai, article_b, 150),
            generate_row(kai, article_b, 180),
            generate_row(basit, article_b, 120),
            generate_row(basit, article_b, 150),
        ]

        df = pd.DataFrame(data)
        prepared_df = helpers.prepare_data(df)
        top_times_per_view = helpers.calculate_default_recs(prepared_df)
        # article a should have 2 minutes per interaction, article b should have 45 seconds per interaction
        assert all(top_times_per_view.index == [article_a, article_b])


def _test_similarities(model):
    similarities = get_similarities(model)
    assert similarities.shape == (3, 3)
    assert all([similarities[i,i] > 0.999 for i in range(similarities.shape[0])])
    return similarities


def _test_weights(external_ids, article_df):
    regular_weights = get_weights(external_ids, article_df, half_life=float('inf'))
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
