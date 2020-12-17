import datetime
import logging

from db.mappings.model import Type
from db.helpers import create_model
from job.helpers import find_or_create_articles, format_ga
from job import preprocessors
from job import trainers
from sites.sites import Sites


def run():
    logging.info("Running job...")

    model_id = create_model(type=Type.ARTICLE.value)
    logging.info(f"Created model with id {model_id}")
    ga_df = preprocessors.fetch_latest_data()
    article_df = find_or_create_articles(Sites.WCP, list(ga_df["page_path"].unique()))
    ga_df = ga_df.join(article_df, on='page_path')

    EXPERIMENT_DATE = datetime.date.today()
    # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
    formatted_df = format_ga(ga_df, date_list=[EXPERIMENT_DATE], half_life=59.631698)
    trainers.train_model(
        formatted_df=formatted_df,
        reg=2.319952,
        n_components=130,
        epochs=2
    )
    article_names = formatted_df.columns
    user_names = formatted_df.index

    logging.info(f"Found or created {len(article_df)} articles")
