from typing import List, Set, TypedDict

import pandas as pd

from db.mappings.model import ModelType
from job.strategies.collaborative_filter.train_model import _spotlight_transform
from job.strategies.collaborative_filter.trainer import Trainer
from job.strategies.templates.strategy import Strategy


class ScrapeConfig(TypedDict):
    """
    Scraping config.
    """

    concurrent_requests: int
    requests_per_second: int


class TrainParamsCF(TypedDict):
    """
    Training params for collaborative filtering.
    """

    hl: int
    embedding_dim: int
    epochs: int
    tune: bool
    tune_params: List[str]
    tune_ranges: List[List[int]]
    model: str
    loss: str


class CollaborativeFiltering(Strategy):
    """
    Collaborative-filtering site configs and methods.
    """

    def __init__(
        self, snowplow_fields: Set[str], scrape_config: ScrapeConfig, training_params: TrainParamsCF, max_article_age
    ):
        super().__init__(model_type=ModelType.ARTICLE)

        # Parameters
        self.snowplow_fields: Set[str] = snowplow_fields
        self.scrape_config: ScrapeConfig = scrape_config
        self.training_params: TrainParamsCF = training_params
        # this is a number of years; will grab dwell time data for any article within the past X years
        self.max_article_age: int = max_article_age

    def fetch_data(self, interactions_data: pd.DataFrame = None):
        self.train_data = interactions_data

    def preprocess_data(self):
        pass

    def generate_embeddings(self):
        model = Trainer(self.train_data, self.experiment_time, _spotlight_transform, self.training_params)
        model.fit()
        self.train_embeddings = model.model_embeddings
        self.decays = model.model_dates_df["date_decays"].values
