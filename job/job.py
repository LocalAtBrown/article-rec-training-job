import logging

from db.models.model import Type
from db.helpers import create_model
from job.helpers import find_or_create_articles
from job import preprocessors
from sites.sites import Sites


def run():
    logging.info("Running job...")

    model_id = create_model(type=Type.ARTICLE.value)
    logging.info(f"Created model with id {model_id}")
    ga_df = preprocessors.fetch_latest_data()
    article_dict = find_or_create_articles(Sites.WCP, list(ga_df["page_path"].unique()))

    logging.info(f"Found or created {len(article_dict)} articles")
