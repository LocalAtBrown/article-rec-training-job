from datetime import datetime
from sites.sites import Sites

import pandas as pd

from job.steps.save_defaults import save_defaults
from tests.base import BaseTest
from db.mappings.recommendation import Rec


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
        article_a = 123
        article_b = 456

        data = [
            generate_row(article_a, exp_date, 1),
            generate_row(article_b, exp_date, 1),
        ]

        df = pd.DataFrame(data)
        model_id = save_defaults(df, site, exp_date)

        default_recs = Rec.select().where(Rec.model == model_id)
        assert len(default_recs) == 2
        assert all([r.source_entity_id == "default" for r in default_recs])
        assert all([r.score == 1 for r in default_recs])
        print(default_recs)
