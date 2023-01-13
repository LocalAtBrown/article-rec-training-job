from typing import Optional

import pandas as pd

from db.helpers import refresh_db
from db.mappings.model import ModelType
from job.helpers import warehouse
from job.strategies.save_defaults import save_defaults
from job.strategies.templates.strategy import Strategy


class Popularity(Strategy):
    """
    Default popularity-model site configs and methods.
    """

    def __init__(self, popularity_window: int):
        super().__init__(model_type=ModelType.POPULARITY)

        # this is a number of days; will only recommend articles within the past X days
        self.popularity_window: Optional[int] = popularity_window
        self.top_articles: Optional[pd.DataFrame] = None

    def fetch_data(self, interactions_data: pd.DataFrame = None) -> None:
        self.top_articles = warehouse.get_default_recs(site=self.site)

    def preprocess_data(self) -> None:
        pass

    def generate_embeddings(self) -> None:
        pass

    def generate_recommendations(self) -> None:
        pass

    @refresh_db
    def save_recommendations(self) -> None:
        save_defaults(self.top_articles, self.site, self.experiment_time)
