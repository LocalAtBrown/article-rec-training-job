from datetime import datetime
import logging
import time

from job.steps import (
    fetch_data,
    scrape_metadata,
    preprocess,
    save_defaults,
    train_model,
    save_predictions,
    delete_old_models,
    warehouse,
)
from job.helpers import get_site
from db.mappings.model import Type
from db.helpers import create_model, set_current_model
from lib.metrics import write_metric, Unit
from lib.config import config
from sites.sites import Sites
import pandas as pd


def run():
    logging.info("Running job...")

    site = get_site(config.get("SITE_NAME"))
    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    status = "success"

    try:
        model_id = create_model(type=Type.ARTICLE.value, site=site.name)
        logging.info(f"Created model with id {model_id}")
        EXPERIMENT_DT = datetime.now()

        data_df = fetch_data.fetch_data(site, EXPERIMENT_DT)
        external_id_df = preprocess.extract_external_ids(
            site, data_df["landing_page_path"].unique().tolist()
        )

        data_df = data_df.merge(external_id_df, on="landing_page_path", how="inner")

        article_df = scrape_metadata.scrape_metadata(
            site, data_df["external_id"].unique().tolist()
        )

        data_df = data_df.join(
            article_df, on="external_id", lsuffix="_original", how="inner"
        )

        warehouse.update_dwell_times(data_df, EXPERIMENT_DT.date(), site)

        data_df = preprocess.filter_activities(data_df)
        data_df = preprocess.filter_articles(data_df)
        article_df = article_df.reset_index()
        save_defaults.save_defaults(data_df, article_df, site.name)

        # Ensure the duration is in seconds and session date is a date (not datetime)
        data_df["session_date"] = data_df["session_date"].dt.date
        data_df["duration"] = data_df["duration"].dt.total_seconds()

        # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
        formatted_df = preprocess.model_preprocessing(
            data_df,
            date_list=[EXPERIMENT_DT.date()],
            half_life=59.631698,
        )

        model = train_model.train_model(
            X=formatted_df, reg=2.319952, n_components=130, epochs=2
        )
        logging.info(f"Successfully trained model on {len(article_df)} inputs.")
        # External IDs to map articles back to
        external_article_ids = formatted_df.columns

        save_predictions.save_predictions(
            model, model_id, external_article_ids, article_df
        )
        set_current_model(model_id, Type.ARTICLE.value, site.name)

        delete_old_models.delete_old_models()
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
