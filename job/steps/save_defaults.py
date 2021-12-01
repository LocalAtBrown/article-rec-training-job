import logging
import pandas as pd

from job.helpers import get_weights
from db.mappings.model import Type
from db.helpers import create_model, set_current_model, create_rec
from lib.config import config
from lib.metrics import write_metric, Unit

MAX_RECS = config.get("MAX_RECS")


# TODO: consider changing default to content similarity calculation
def calculate_default_recs(prepared_df: pd.DataFrame) -> pd.Series:
    # TODO consider changing this to a larger time span (1 day of data is ~100k)
    TOTAL_INTERACTIONS = 5000
    timed_interactions = prepared_df[~prepared_df.duration.isna()]
    recent_interactions = timed_interactions.nlargest(
        n=TOTAL_INTERACTIONS, columns=["activity_time"]
    )
    times = (
        recent_interactions[["external_id", "duration"]]
        .groupby("external_id")
        .sum()
        .sort_index()
    )
    pageviews = (
        recent_interactions[["external_id", "client_id"]]
        .groupby("external_id")
        .nunique("client_id")
        .sort_index()
    )
    times_per_view = times.duration / pageviews.client_id
    top_times_per_view = times_per_view.sort_values(ascending=False)
    return top_times_per_view


def save_defaults(
    prepared_df: pd.DataFrame, article_df: pd.DataFrame, site_name: str
) -> None:
    top_times_per_view = calculate_default_recs(prepared_df)
    weights = get_weights(top_times_per_view.index, article_df)
    # the most read article will have a perfect score of 1.0, all others will be a fraction of that
    scores = weights * top_times_per_view / max(top_times_per_view)
    top_scores = scores.nlargest(MAX_RECS)
    model_id = create_model(type=Type.POPULARITY.value, site=site_name)
    logging.info("Saving default recs to db...")
    for external_id, score in zip(top_times_per_view.index, top_scores):
        matching_articles = article_df[article_df["external_id"] == external_id]
        article_id = matching_articles["article_id"].tolist()[0]
        rec_id = create_rec(
            source_entity_id="default",
            model_id=model_id,
            recommended_article_id=article_id,
            score=score,
        )

    set_current_model(model_id, Type.POPULARITY.value, site_name)
