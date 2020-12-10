import time
import logging

from db.models.model import Type
from db.helpers import create_model
from job.helpers import find_or_create_articles
from job import preprocessors
from sites.sites import Sites
from lib.metrics import metrics


def run():
    logging.info("Running job...")
    start = time.time()

    model_id = create_model(type=Type.ARTICLE.value)
    logging.info(f"Created model with id {model_id}")
    ga_df = preprocessors.fetch_latest_data()
    article_dict = find_or_create_articles(Sites.WCP, list(ga_df["page_path"].unique()))

    logging.info(f"Found or created {len(article_dict)} articles")

    job_timing = int((time.time() - start) * 1000)
    metrics.timing("job_timing_ms", job_timing, tags={"status": "success"})
