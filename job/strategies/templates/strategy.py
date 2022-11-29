from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List

import numpy as np
import pandas as pd

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from sites.templates.site import Site


class Strategy(metaclass=ABCMeta):
    @abstractmethod
    def fetch_data(site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def preprocess_data(site: Site, article_data: List[Dict[str]], interactions_data: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def generate_embeddings(train_data: pd.DataFrame) -> np.ndarray:
        pass

    @abstractmethod
    def generate_recommendations(train_embeddings: np.ndarray, train_data: pd.DataFrame) -> List[Rec]:
        pass

    @abstractmethod
    def save_recommendations(site: Site, recs: List[Rec], model_type: ModelType) -> None:
        pass
