from dataclasses import dataclass
from typing import Any, Dict, List, Set, TypedDict

import numpy as np
import pandas as pd

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
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


@dataclass
class CollaborativeFiltering(Strategy):
    """
    Collaborative-filtering site configs and methods.
    """

    snowplow_fields: Set[str]
    scrape_config: ScrapeConfig
    training_params: TrainParamsCF
    # this is a number of years; will grab dwell time data for any article within the past X years
    max_article_age: int

    def fetch_data(self, site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
        pass

    def preprocess_data(
        self, site: Site, article_data: List[Dict[str, str]], interactions_data: pd.DataFrame
    ) -> pd.DataFrame:
        pass

    def generate_embeddings(self, train_data: pd.DataFrame) -> np.ndarray:
        pass

    def generate_recommendations(self, train_embeddings: np.ndarray, train_data: pd.DataFrame) -> List[Rec]:
        pass

    def save_recommendations(self, site: Site, recs: List[Rec], model_type: ModelType) -> None:
        pass
