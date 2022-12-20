from datetime import datetime

import pandas as pd

from db.helpers import refresh_db
from db.mappings.model import ModelType
from job.helpers import warehouse
from job.strategies.save_defaults import save_defaults
from job.strategies.templates.strategy import Strategy
from sites.templates.site import Site


class Popularity(Strategy):
    """
    Default popularity-model site configs and methods.
    """

    # this is a number of days; will only recommend articles within the past X days
    popularity_window: int
    top_articles: pd.DataFrame
    experiment_time: datetime
    model_type: ModelType = ModelType.POPULARITY

    def __init__(self, popularity_window: int):
        self.popularity_window = popularity_window

    def fetch_data(self, site: Site, interactions_data: pd.DataFrame = None, experiment_time=None) -> None:
        self.site = site
        self.experiment_time = experiment_time
        self.top_articles = warehouse.get_default_recs(site=site)

    def preprocess_data(self) -> None:
        pass

    def generate_embeddings(self) -> None:
        pass

    def generate_recommendations(self) -> None:
        """
        Run article-level embeddings through a KNN and create recs from resulting neighbors.
        """
        pass

    @refresh_db
    def save_recommendations(self, site: Site) -> None:
        save_defaults(self.top_articles, site, self.experiment_time)
