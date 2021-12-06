import logging
import time
from typing import List

import pandas as pd

from db.helpers import create_rec
from job.steps.implicit_mf import ImplicitMF
from job.helpers import get_similarities, get_weights, get_orders
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")


def save_predictions(
    model: ImplicitMF, model_id: int, external_ids: List[str], article_df: pd.DataFrame
):
    start_ts = time.time()
    created_recs = 0
    vector_similarities = get_similarities(model)
    vector_weights = get_weights(external_ids, article_df)
    vector_orders = get_orders(vector_similarities, vector_weights)

    for source_index, ranked_recommendation_indices in enumerate(vector_orders):
        source_external_id = external_ids[source_index]

        rec_ids = set()
        for recommendation_index in ranked_recommendation_indices[: MAX_RECS + 1]:
            if len(rec_ids) == MAX_RECS:
                break

            recommended_external_id = external_ids[recommendation_index]
            # If it's the article itself, skip it
            if recommended_external_id == source_external_id:
                continue

            matching_articles = article_df[
                article_df["external_id"] == recommended_external_id
            ]
            recommended_article_id = matching_articles["article_id"].tolist()[0]
            # for distance, smaller values are more highly correlated
            # for score, higher values are more highly correlated
            score = vector_similarities[source_index][recommendation_index]
            # fix case when some scores are negative due to a rounding error
            score = max(score, 0.0)

            rec_id = create_rec(
                source_entity_id=source_external_id,
                model_id=model_id,
                recommended_article_id=recommended_article_id,
                score=score,
            )
            rec_ids.add(rec_id)
            created_recs += 1
            if created_recs % 1000 == 0:
                logging.info(f"Created {created_recs} recommendations...")

    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", created_recs)
