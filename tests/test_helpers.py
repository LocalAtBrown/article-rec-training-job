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
    model =  ImplicitFactorizationModel(n_iter=35, random_state=np.random.RandomState(42), embedding_dim=16)
    model.fit(dataset, verbose=True)
    return model

@pytest.fixture(scope="module")
def user_ids():
    return np.array([3,3,2,1,3,3,1,2])

@pytest.fixture(scope="module")
def spotlight_ids():
    return np.array([4,3,3,3,1,2,1,2])

@pytest.fixture(scope="module")
def ratings():
    return np.array([2,3,0,4,3,0,3,0])

@pytest.fixture(scope="module")
def publish_dates():
    return np.array([1,1,2,1,2,3,2,3])

@pytest.fixture(scope="module")
def article_ids():
    return np.array([14,13,13,13,11,12,11,12])


def _test_similarities(embeddings:np.ndarray, n_recs:int):
    """ Checks that n recs are returned and the most similar rec is identical"""
    distances, indices = get_similarities(embeddings, n_recs)
    assert distances.shape == (4, n_recs)
    assert all([distances[i, 0] == 0. for i in range(distances.shape[0])])
    return distances, indices


def _test_orders(n_recs:int, nearest_indices:np.ndarray, distances:np.ndarray, article_ids:np.ndarray):
    """Gets the most similar recs for spotlight_id = 1, converted to article_ids (DB id format)
        The most similar recs should be 13 and 14.

        Here is why:
            Looking at spotlight_id = 1, we see two users implicity rated: user_id 1 and user_id 3. 
            We also see that users 1 and 3 also consumed spotlight_id 3 and gave similar, high ratings
            We also see that user_id 3 also consumed ids 2 and 4. 2  was low rated, 4 got a high ratings
            So because of their user consumption connections, spotlight_ids 3 and 4 are our recs 
            3 is higher rated and consumed by both user_ids 1 and 3. So it is our top pick.
            We map back to 13 and 14 because our spotlight ids match index-wise to the LNL db ids (article_ids)
            The get_nearest function performs the mapping
    """
    _test_spotlight_id = 1
    rec_ids, rec_distances = get_nearest(_test_spotlight_id, nearest_indices, distances, article_ids) 
    assert rec_ids.shape == (n_recs - 1,)
    assert (rec_ids == np.array([13,14])).all()
    return rec_ids, rec_distances 


def test_article_recommendations(spotlight_ids, user_ids, ratings, publish_dates, article_ids):
    n_recs = 3
    dataset = generate_interactions(pd.DataFrame({'user_id': user_ids,
                                                'item_id': spotlight_ids,
                                                'ratings': ratings,
                                                'timestamp': publish_dates}))
    model = build_model(dataset)
    embeddings = get_model_embeddings(model, np.unique(spotlight_ids))
    distances, nearest_indices = _test_similarities(embeddings, n_recs)
    nearest_recs = _test_orders(n_recs, nearest_indices, distances, np.unique(article_ids))
    
