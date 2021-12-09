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
from db.mappings.base import db_proxy
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
        EXPERIMENT_DT = datetime.now()

        data_df = fetch_data.fetch_data(site, EXPERIMENT_DT)
        logging.info("Fetched data")
        external_id_df = preprocess.extract_external_ids(
            site, data_df["landing_page_path"].unique().tolist()
        )

        data_df = data_df.merge(external_id_df, on="landing_page_path", how="inner")
      
        db_proxy.close() 
        db_proxy.connect()

        article_df = scrape_metadata.scrape_metadata(
            site, data_df["external_id"].unique().tolist()
        )

        data_df = data_df.join(
            article_df, on="external_id", lsuffix="_original", how="inner"
        )

        #warehouse.update_dwell_times(data_df, EXPERIMENT_DT.date(), site)
        EXPERIMENT_DT = datetime.now().date()
        data_df, article_df = fetch_and_upload_data(site, EXPERIMENT_DT)

        data_df = preprocess.filter_activities(data_df)
        data_df = preprocess.filter_articles(data_df)
        article_df = article_df.reset_index()
        save_defaults.save_defaults(data_df, article_df, site.name)

        wh_data = warehouse.get_dwell_times(site, days=config.get("DAYS_OF_DATA"))

        model, external_item_ids, spotlight_ids, article_ids, date_decays = train_model.train_model(
            X=wh_data, params=site.params, time=EXPERIMENT_DT
                )
        logging.info(f"Successfully trained model on {len(wh_data)} inputs.")

        save_predictions.save_predictions(
            model=model, model_id=model_id, 
            spotlight_ids=spotlight_ids, 
            external_item_ids=external_item_ids,
            article_ids=article_ids,
            date_decays=date_decays
        )
        set_current_model(model_id, Type.ARTICLE.value, site.name)

        delete_old_models.delete_old_models()
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
