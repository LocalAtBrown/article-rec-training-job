from datetime import datetime
from typing import List, Set, TypedDict

import pandas as pd

from db.mappings.model import ModelType
from job.strategies.collaborative_filter.train_model import _spotlight_transform
from job.strategies.collaborative_filter.trainer import Trainer
from job.strategies.templates.strategy import Strategy
from sites.templates.site import Site


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

    interactions_data: pd.DataFrame
    experiment_time: datetime
    snowplow_fields: Set[str]
    scrape_config: ScrapeConfig
    training_params: TrainParamsCF
    model_type: ModelType = ModelType.ARTICLE
    # this is a number of years; will grab dwell time data for any article within the past X years
    max_article_age: int

    def __init__(
        self, snowplow_fields: Set[str], scrape_config: ScrapeConfig, training_params: TrainParamsCF, max_article_age
    ):
        self.snowplow_fields = snowplow_fields
        self.scrape_config = scrape_config
        self.training_params = training_params
        self.max_article_age = max_article_age

    def fetch_data(self, site: Site, interactions_data: pd.DataFrame = None, experiment_time=None):
        self.interactions_data = interactions_data
        self.experiment_time = experiment_time
        pass

    def preprocess_data(self, site: Site):
        pass

    def generate_embeddings(self):
        model = Trainer(self.interactions_data, self.experiment_time, _spotlight_transform, self.training_params)
        model.fit()
        self.train_embeddings = model.model_embeddings
        self.decays = model.model_dates_df["date_decays"].values
