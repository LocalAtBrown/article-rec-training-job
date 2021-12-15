import numpy as np
import pandas as pd
import pytest

from datetime import datetime, timedelta
from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

from db.mappings.base import tzaware_now
from job.steps.save_predictions import get_similarities, get_nearest
from job.steps.trainer import Trainer
from job.steps.train_model import _spotlight_transform


@pytest.fixture(scope="module")
def user_ids():
    return np.array([3,3,2,1,3,3,1,2])

@pytest.fixture(scope="module")
def external_ids():
    return np.array([4,3,3,3,1,2,1,2])

@pytest.fixture(scope="module")
def durations():
    return np.array([2,3,0,4,3,0,3,0])

@pytest.fixture(scope="module")
def session_dates():
    return np.array(["12/13/2020","12/13/2020","12/14/2020","12/13/2020","12/14/2020","12/15/2020","12/14/2020","12/15/2020"])

@pytest.fixture(scope="module")
def publish_dates():
    return np.array(["12/14/2020","12/13/2020","12/13/2020","12/13/2020","12/11/2020","12/12/2020","12/11/2020","12/12/2020"])

@pytest.fixture(scope="module")
def article_ids():
    return np.array([14,13,13,13,11,12,11,12])

@pytest.fixture(scope="module")
def decays():
    return np.array([0.9,1,1,1,0.95,0.98,0.95,0.98])

def _test_similarities(embeddings:np.ndarray, n_recs:int, decays:np.ndarray):
    """ Checks that n recs are returned and the most similar rec is identical"""
    similarities, indices = get_similarities(embeddings, np.unique(decays), n_recs)
    assert similarities.shape == (4, n_recs)
    assert all([similarities[i, 0] == 1. for i in range(similarities.shape[0])])
    return similarities, indices


def _test_orders(n_recs:int, nearest_indices:np.ndarray, similarities:np.ndarray, article_ids:np.ndarray):
    """Gets the most similar recs for spotlight_id = 1, converted to article_ids (DB id format)
        The most similar recs should be 13 and 14.

        Here is why:
            Looking at spotlight_id = 1, we see two users implicity rated: user_id 1 and user_id 3. 
            We also see that users 1 and 3 also consumed spotlight_id 3 and gave similar, high durations 
            We also see that user_id 3 also consumed ids 2 and 4. 2  was low rated, 4 got a high duration and slightly below 3
            However, we see that another user, 2, who consumed 3 and shared patterns with 1, gave 3 a lower rating
            So because of their user consumption connections, spotlight_ids 3 and 4 are our recs 
            4 has more consistent ratings than 3. So it is our top pick.
            We map back to 14 and 13 because our spotlight ids match index-wise to the LNL db ids (article_ids)
            The get_nearest function performs the mapping
    """
    _test_spotlight_id = 1
    rec_ids, rec_similarities = get_nearest(_test_spotlight_id, nearest_indices, similarities, article_ids) 
    assert rec_ids.shape == (n_recs - 1,)
    assert (rec_ids == np.array([14,13])).all()
    return rec_ids, rec_distances 


def test_article_recommendations(spotlight_ids, user_ids, durations, publish_dates, article_ids, decays):
    n_recs = 3
    dataset = generate_interactions(pd.DataFrame({'user_id': user_ids,
                                                'item_id': spotlight_ids,
                                                'duration': durations,
                                                'timestamp': publish_dates}))
    model = build_model(dataset)
    embeddings = get_model_embeddings(model, np.unique(spotlight_ids))
    similarities, nearest_indices = _test_similarities(embeddings, n_recs, decays)
    nearest_recs = _test_orders(n_recs, nearest_indices, similarities, np.unique(article_ids))
    
