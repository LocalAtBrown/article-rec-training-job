import datetime
import logging
import time

from db.mappings.model import Type
from db.helpers import create_model, set_current_model
from job import preprocessors, models
from job.helpers import (
    create_article_to_article_recs,
    create_default_recs,
    find_or_create_articles,
    format_data,
    prepare_data,
)
from job.steps import fetch
from sites.sites import Sites
from lib.metrics import write_metric, Unit


def run():
    logging.info("Running job...")
    start_ts = time.time()
    status = "success"
    try:
        model_id = create_model(type=Type.ARTICLE.value)
        logging.info(f"Created model with id {model_id}")
        data_df = fetch.fetch()

        article_df = find_or_create_articles(
            Sites.WCP, list(data_df.landing_page_path.unique())
        )
        data_df = data_df.join(article_df, on="landing_page_path")
        prepared_df = prepare_data(data_df)

        create_default_recs(prepared_df, article_df)

        EXPERIMENT_DATE = datetime.date.today()
        # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
        formatted_df = format_data(
            prepared_df, date_list=[EXPERIMENT_DATE], half_life=59.631698
        )
        model = models.train_model(
            X=formatted_df, reg=2.319952, n_components=130, epochs=2
        )
        logging.info(f"Successfully trained model on {len(article_df)} inputs.")

        # External IDs to map articles back to
        external_article_ids = formatted_df.columns
        external_article_ids = external_article_ids.astype("int32")
        external_user_ids = formatted_df.index

        create_article_to_article_recs(
            model, model_id, external_article_ids, article_df
        )
        set_current_model(model_id, Type.ARTICLE.value)
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
