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
from sites.site import Site
import pandas as pd


def fetch_and_upload_data(
    site: Site, date: datetime.date, days=config.get("DAYS_OF_DATA")
):
    """
    Fetch data from S3
    Dwell time calculation
    Hydrate with article metadata
    Upload to data warehouse
    Return df of data, and articles
    """
    data_df = fetch_data.fetch_data(site, date, days)
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

    warehouse.update_dwell_times(data_df, date, site)
    return data_df, article_df


def run():
    logging.info("Running job...")

    site = get_site(config.get("SITE_NAME"))
    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    status = "success"

    try:
        model_id = create_model(type=Type.ARTICLE.value, site=site.name)
        logging.info(f"Created model with id {model_id}")
        EXPERIMENT_DT = datetime.now().date()

        ## Step 1: Fetch fresh data, hydrate it, and upload it to the warehouse
        data_df, article_df = fetch_and_upload_data(site, EXPERIMENT_DT)

        ## Step 2: Train models by pulling data from the warehouse and uploading
        ## new recommendation objects
        top_articles = warehouse.get_default_recs(site=site)
        save_defaults.save_defaults(top_articles, site, EXPERIMENT_DT)

        interactions_data = warehouse.get_dwell_times(
            site, days=config.get("DAYS_OF_DATA")
        )

        model, dates_df = train_model.train_model(
            X=interactions_data, params=site.training_params, time=EXPERIMENT_DT
        )

        logging.info(f"Successfully trained model on {len(interactions_data)} inputs.")

        save_predictions.save_predictions(
            model=model,
            model_id=model_id,
            spotlight_ids=dates_df["item_id"].values,
            external_item_ids=dates_df["external_id"].values,
            article_ids=dates_df["article_id"].values,
            date_decays=dates_df["date_decays"].values,
        )
        set_current_model(model_id, Type.ARTICLE.value, site.name)

        delete_old_models.delete_old_models()
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
