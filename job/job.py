import datetime
import logging

from db.mappings.model import Type
from db.helpers import create_model, set_current_model
from job.helpers import (
    find_or_create_articles,
    format_ga,
    create_article_to_article_recs,
    create_default_recs,
)
from job import preprocessors
from job import models
from sites.sites import Sites


def run():
    logging.info("Running job...")

    model_id = create_model(type=Type.ARTICLE.value)
    logging.info(f"Created model with id {model_id}")
    ga_df = preprocessors.fetch_latest_data()
    article_df = find_or_create_articles(Sites.WCP, list(ga_df["landing_page_path"].unique()))
    ga_df = ga_df.join(article_df, on="page_path")

    create_default_recs(ga_df, article_df)

    EXPERIMENT_DATE = datetime.date.today()
    # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
    formatted_df = format_ga(ga_df, date_list=[EXPERIMENT_DATE], half_life=59.631698)
    model = models.train_model(X=formatted_df, reg=2.319952, n_components=130, epochs=2)
    logging.info(f"Successfully trained model on {len(article_df)} inputs.")

    # External IDs to map articles back to
    external_article_ids = formatted_df.columns
    external_user_ids = formatted_df.index

    create_article_to_article_recs(model, model_id, external_article_ids, article_df)
    set_current_model(model_id, Type.ARTICLE.value)
