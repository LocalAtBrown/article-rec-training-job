from __future__ import annotations

from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List

import numpy as np
import pandas as pd

from db.mappings.model import ModelType
from db.mappings.recommendation import Rec

if TYPE_CHECKING:
    from sites.templates.site import Site


@dataclass
class Strategy(metaclass=ABCMeta):
    @abstractmethod
    def fetch_data(self, site: Site, interactions_data: pd.DataFrame) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def preprocess_data(
        self, site: Site, article_data: List[Dict[str, str]], interactions_data: pd.DataFrame
    ) -> pd.DataFrame:
        pass

    @abstractmethod
    def generate_embeddings(self, train_data: pd.DataFrame) -> np.ndarray:
        pass

    @abstractmethod
    def generate_recommendations(self, train_embeddings: np.ndarray, train_data: pd.DataFrame) -> List[Rec]:
        pass

    @abstractmethod
    def save_recommendations(self, site: Site, recs: List[Rec], model_type: ModelType) -> None:
        pass
