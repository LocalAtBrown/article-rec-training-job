import pandas as pd

from job.strategies.templates.strategy import Strategy
from sites.templates.site import Site


class Popularity(Strategy):
    """
    Default popularity-model site configs and methods.
    """

    def __init__(self, popularity_window):
        # this is a number of days; will only recommend articles within the past X days
        self.popularity_window: int = popularity_window

    def fetch_data(self, site: Site, interactions_data: pd.DataFrame = None) -> None:
        pass

    def preprocess_data(self) -> None:
        pass

    def generate_embeddings(self) -> None:
        pass
