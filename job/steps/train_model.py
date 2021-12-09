import logging
import numpy as np
import pandas as pd
import pdb
from copy import deepcopy

from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

DEFAULT_PARAMS = {
        "hl": 10,
        "epochs": 20,
        "embedding_dim": 72
        }

def generate_interactions(prepared_df:pd.DataFrame):
    """ Generate an Interactions object"""
    dataset = Interactions(user_ids=prepared_df['user_id'].values,
                       item_ids=prepared_df['item_id'].values,
                       ratings=prepared_df['duration'].values,
                       timestamps=prepared_df['timestamp'].values)
    return dataset

def spotlight_transform(prepared_df:pd.DataFrame, 
                        half_life:float, 
                        experiment_time: pd.Timestamp):
    """Transform data for Spotlight

    :prepared_df: Dataframe with user-article interactions
    :half_life: time decay for articles 
    :experiment_time: time to benchmark decay against
    :return: (dataset, external_id uniques, item_id uniques, article_id uniques)
    """
    prepared_df = prepared_df.dropna()

    experiment_time = pd.to_datetime(experiment_time)
    prepared_df["published_at"] = pd.to_datetime(prepared_df["published_at"])
    prepared_df["published_at"] = (experiment_time - prepared_df["published_at"]).dt.days
    prepared_df["session_date"] = pd.to_datetime(prepared_df["session_date"])
    prepared_df["session_date"] = prepared_df["session_date"].dt.date
    prepared_df['external_id'] = prepared_df['external_id'].astype('category')
    prepared_df['item_id'] = prepared_df['external_id'].cat.codes + 1
    prepared_df['user_id'] = prepared_df['client_id'].factorize()[0] + 1
    prepared_df['timestamp'] = prepared_df['session_date'].factorize()[0] + 1
    experiment_time = pd.to_datetime(experiment_time)
    prepared_df.reset_index(inplace=True, drop=True) 

    dataset = generate_interactions(prepared_df)

    return (dataset, 
            prepared_df['external_id'].unique(), 
            prepared_df['item_id'].unique(), 
            prepared_df['article_id'].unique(), 
            prepared_df["published_at"].unique()) 

def get_hparams(params:dict = None):
    _params = deepcopy(DEFAULT_PARAMS)
    _params.update(params)
    return _params

def train_model(X:pd.DataFrame, params:dict, time=pd.datetime):
    """Train spotlight model

    X: pandas dataframe of interactions
    params: Hyperparameters
    time: time to benchmark decay against

    return: (model: Spotlight model, 
            external_item_ids: Publisher unique article IDs, 
            internal_ids: Spotlight article IDs, 
            article_ids: LNL DB article IDs)
    """
    dataset, external_item_ids, spotlight_ids, article_ids, date_offsets = spotlight_transform(prepared_df=X, half_life=params["hl"], experiment_time=time)
    params = get_hparams(params)

    model = ImplicitFactorizationModel(n_iter=params["epochs"], embedding_dim=params["embedding_dim"])
    model.fit(dataset, verbose=True)

    return model, external_item_ids, spotlight_ids, article_ids, date_offsets
