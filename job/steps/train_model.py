import logging
import numpy as np
import pandas as pd

from spotlight.factorization.implicit import ImplicitFactorizationModel
from spotlight.interactions import Interactions

def spotlight_transform(prepared_df:pd.DataFrame, 
                        half_life:float, 
                        current_time: pd.Timestamp):
    """Transform data for Spotlight

    :prepared_df: Dataframe with user-article interactions
    :half_life: time decay for articles 
    :current_time: time to benchmark decay against
    :return: (dataset, external_id uniques, item_id uniques, article_id uniques)
    """
    prepared_df = prepared_df[['external_id', 'article_id', 'client_id','session_date', 'duration']]
    prepared_df = prepared_df.dropna()
    
    prepared_df['external_id'] = prepared_df['external_id'].astype('category')
    prepared_df['item_id'] = prepared_df['external_id'].cat.codes + 1
    prepared_df['user_id'] = prepared_df['client_id'].factorize()[0] + 1
    prepared_df['timestamp'] = prepared_df['session_date'].factorize()[0] + 1
    prepared_df['ratings'] = prepared_df['duration'].dt.total_seconds()
    prepared_df['ratings'] = prepared_df['ratings'].clip(upper=600).astype(np.int32)
    current_date = pd.to_datetime(current_time)
    # exponential decay
    prepared_df['ratings'] = prepared_df['ratings'] * (0.5**((current_date - prepared_df['session_date']).dt.days / half_life))

    prepared_df.reset_index(inplace=True, drop=True) 

    dataset = Interactions(user_ids=prepared_df['user_id'].values,
                       item_ids=prepared_df['item_id'].values,
                       ratings=prepared_df['ratings'].values,
                       timestamps=prepared_df['timestamp'].values)

    return (dataset, prepared_df['external_id'].unique(), prepared_df['item_id'].unique(), prepared_df['article_id'].unique()) 

def train_model(X:pd.DataFrame, params:dict, time=pd.datetime):
    """Train spotlight model

    X: pandas dataframe of interactions
    params: Hyperparameters
    time: time to benchmark decay against

    return: (model: Spotlight model, 
            external_item_ids: ARC CMS article IDs, 
            internal_ids: Spotlight article IDs, 
            article_ids: LNL DB article IDs)
    """
    dataset, external_item_ids, internal_ids, article_ids = spotlight_transform(prepared_df=X, half_life=params["hl"], current_time=time) 

    model = ImplicitFactorizationModel(n_iter=params["epochs"], embedding_dim=params["embeddings_dim"])
    model.fit(dataset, verbose=True)

    return model, external_item_ids, internal_ids, article_ids
