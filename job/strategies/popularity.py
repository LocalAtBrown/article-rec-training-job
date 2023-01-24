import logging
from typing import Optional

import numpy as np
import pandas as pd

from db.helpers import create_model, refresh_db, set_current_model
from db.mappings.base import db_proxy
from db.mappings.model import ModelType
from db.mappings.recommendation import Rec
from job.helpers import warehouse
from job.helpers.datetime import time_decay
from job.strategies.templates.strategy import Strategy
from lib.config import config

MAX_RECS = config.get("MAX_RECS")


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
        decayed_df = time_decay(
            self.top_articles,
            experiment_date=self.experiment_time,
            half_life=10,
            date_col="publish_date",
            duration_col="score",
        )
        self.top_articles["score"] /= np.max(self.top_articles["score"])
        self.top_articles = self.top_articles.nlargest(n=MAX_RECS, columns="score")

        model_id = create_model(type=ModelType.POPULARITY.value, site=self.site.name)
        logging.info(f"Model ID: {model_id}")

        to_create = []
        for _, row in decayed_df.iterrows():
            to_create.append(
                Rec(
                    source_entity_id="default",
                    model_id=model_id,
                    recommended_article_id=row.article_id,
                    score=row.score,
                )
            )
        logging.info(f"Saving {len(to_create)} default recs to db...")

        with db_proxy.atomic():
            Rec.bulk_create(to_create, batch_size=50)

        set_current_model(model_id, ModelType.POPULARITY, model_site=self.site.name)
