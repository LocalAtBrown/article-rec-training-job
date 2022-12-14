from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List

import numpy as np
import pandas as pd

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec

if TYPE_CHECKING:
    from sites.templates.site import Site


class Strategy(metaclass=ABCMeta):
    """
    Superclass that defines the methods for each one of the recommendation strategies (collaborative filtering,
    popularity, semantic similarity).
    """

    @abstractmethod
    def fetch_data(self, site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Fetch data from the data warehouse
        """
        pass

    @abstractmethod
    def preprocess_data(
        self, site: Site, article_data: List[Dict[str, str]], interactions_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Preprocess fetched data into a DataFrame ready for training.
        """
        pass

    @abstractmethod
    def generate_embeddings(self, train_data: pd.DataFrame) -> np.ndarray:
        """
        Given DataFrame with training data, create embeddings.
        """
        pass

    @abstractmethod
    def generate_recommendations(self, train_embeddings: np.ndarray, train_data: pd.DataFrame) -> List[Rec]:
        """
        Run article-level embeddings through a KNN and create recs from resulting neighbors.
        """
        pass

    @abstractmethod
    def save_recommendations(self, site: Site, recs: List[Rec], model_type: ModelType) -> None:
        """
        Save generated recommendations to database.
        """
        pass
