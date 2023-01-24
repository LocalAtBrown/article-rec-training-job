from datetime import datetime

import pandas as pd

from db.mappings.recommendation import Rec
from job.strategies.popularity import Popularity
from sites.sites import Sites
from tests.base import BaseTest


def generate_row(article_id, publish_date, score):
    return {
        "article_id": article_id,
        "score": score,
        "publish_date": publish_date,
    }


class TestSaveDefaults(BaseTest):
    def test_save_defaults(self) -> None:
        exp_date = datetime.now().date()
        site = Sites.WCP
        popularity = Popularity(popularity_window=28)
        article_a = 123
        article_b = 456

        data = [
            generate_row(article_a, exp_date, 1),
            generate_row(article_b, exp_date, 1),
        ]

        df = pd.DataFrame(data)
        popularity.top_articles = df
        popularity.prepare(site=site, experiment_time=pd.to_datetime(exp_date))
        popularity.save_recommendations()

        default_recs = Rec.select().where(Rec.model == popularity.model_id)
        assert len(default_recs) == 2
        assert all([r.source_entity_id == "default" for r in default_recs])
        assert all([r.score == 1 for r in default_recs])
        print(default_recs)
