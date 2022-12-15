from dataclasses import dataclass
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.strategies.templates.strategy import Strategy
from sites.templates.site import Site


@dataclass
class Popularity(Strategy):
    """
    Default popularity-model site configs and methods.
    """

    # this is a number of days; will only recommend articles within the past X days
    popularity_window: int

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
