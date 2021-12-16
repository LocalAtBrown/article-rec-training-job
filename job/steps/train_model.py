import pandas as pd

from job.steps.trainer import Trainer


def _spotlight_transform(prepared_df:pd.DataFrame) -> pd.DataFrame:
    """Transform data for Spotlight
    :prepared_df: Dataframe with user-article interactions
    :return: (prepared_df)
    """
    prepared_df = prepared_df.dropna()
    prepared_df["published_at"] = pd.to_datetime(prepared_df["published_at"])
    prepared_df["session_date"] = pd.to_datetime(prepared_df["session_date"])
    prepared_df["session_date"] = prepared_df["session_date"].dt.date
    prepared_df['external_id'] = prepared_df['external_id'].astype('category')
    prepared_df['item_id'] = prepared_df['external_id'].cat.codes
    prepared_df['user_id'] = prepared_df['client_id'].factorize()[0]
    prepared_df['timestamp'] = prepared_df['session_date'].factorize()[0] + 1

    return prepared_df

def train_model(X:pd.DataFrame, params:dict, experiment_time=pd.datetime) -> (Trainer, pd.DataFrame):
    """Train spotlight model

    X: pandas dataframe of interactions
    params: Hyperparameters
    experiment_time: time to benchmark decay against

    return: (embeddings: Spotlight model embeddings for each unique article, 
            dates_df: pd.DataFrame with columns:
                external_item_ids: Publisher unique article IDs, 
                internal_ids: Spotlight article IDs, 
                article_ids: LNL DB article IDs,
                date_decays: Decay factors for each article [0,1]
            )
    """
    model = Trainer(X, experiment_time, _spotlight_transform, params)
    model.fit()
    return (model.model_embeddings, model.model_dates_df)
