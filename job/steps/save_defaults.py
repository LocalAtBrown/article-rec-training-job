import logging
import pandas as pd
import numpy as np
import datetime

from job.helpers import get_weights
from db.mappings.model import Type
from db.helpers import create_model, set_current_model, create_rec
from job.steps import preprocess
from lib.config import config
from sites.site import Site
from lib.metrics import write_metric, Unit
from db.mappings.recommendation import Rec
from db.mappings.base import db_proxy

MAX_RECS = config.get("MAX_RECS")


def save_defaults(
    top_articles: pd.DataFrame, site: Site, experiment_date: datetime.datetime.date
) -> None:
    decayed_df = preprocess.time_decay(
        top_articles,
        experiment_date=experiment_date,
        half_life=10,
        date_col="publish_date",
        duration_col="score",
    )
    top_articles["score"] /= np.max(top_articles["score"])
    top_articles = top_articles.nlargest(n=MAX_RECS, columns="score")

    model_id = create_model(type=Type.POPULARITY.value, site=site.name)

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

    set_current_model(model_id, Type.POPULARITY.value, model_site=site.name)
    return model_id
