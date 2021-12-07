import numpy as np
import pandas as pd
import pytest

from datetime import datetime, timedelta
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions
from db.mappings.base import tzaware_now
from job.steps.save_predictions import get_similarities, get_nearest, get_model_embeddings
from job.steps.train_model import generate_interactions 


def build_model(dataset):
    model =  ImplicitFactorizationModel(n_iter=5, random_state=np.random.RandomState(42), embedding_dim=8)
    model.fit(dataset)
    return model


@pytest.fixture(scope="module")
def user_ids():
    return np.array([1,1,2,1,3,3,1,2])

@pytest.fixture(scope="module")
def item_ids():
    return np.array([4,4,1,2,1,2,3,2])

@pytest.fixture(scope="module")
def ratings():
    return np.array([1,2,0,1,3,1,2,1])

@pytest.fixture(scope="module")
def publish_dates():
    return np.array([3,1,2,1,2,1,2,3])

@pytest.fixture(scope="module")
def article_ids():
    return np.array([14,14,11,12,11,12,13,12])

@pytest.fixture(scope="module")
def external_ids():
    return np.array(["24","24","21","22","21","22","23","22"])


def _test_similarities(embeddings:np.ndarray, n_recs:int):
    distances, indices = get_similarities(embeddings, n_recs)
    assert distances.shape == (4, n_recs)
    assert all([distances[i, 0] == 0. for i in range(distances.shape[0])])
    return distances, indices


def _test_orders(n_recs:int, indices:np.ndarray, distances:np.ndarray, article_ids:np.ndarray):
    rec_indexes, rec_distances = get_nearest(1, indices, distances, article_ids) 
    assert rec_indexes.shape == (n_recs - 1,)
    assert (rec_indexes == np.array([13,14])).all()
    return rec_indexes, rec_distances 


def test_article_recommendations(item_ids, user_ids, ratings, publish_dates, article_ids, external_ids):
    n_recs = 3
    dataset = generate_interactions(pd.DataFrame({'user_id': user_ids,
                                                'item_id': item_ids,
                                                'ratings': ratings,
                                                'timestamp': publish_dates}))
    model = build_model(dataset)
    embeddings = get_model_embeddings(model, np.unique(item_ids))
    distances, indices = _test_similarities(embeddings, n_recs)
    nearest_recs = _test_orders(n_recs, indices, distances, np.unique(article_ids))
    
