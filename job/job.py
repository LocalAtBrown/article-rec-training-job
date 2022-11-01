import logging
import time
from datetime import datetime, timedelta

from job.helpers.site import get_site
from job.steps.collaborative_filtering import (
    fetch_data,
    save_defaults,
    save_predictions,
    scrape_metadata,
    train_model,
    warehouse,
)
from lib.config import config
from lib.metrics import Unit, write_metric
from sites.site import Site


def fetch_and_upload_data(site: Site, dt: datetime, hours=config.get("HOURS_OF_DATA")):
    """
    1. Upload transformed events data to Redshift
    2. Update article metadata
    3. Update dwell times table
    """
    dts = [dt - timedelta(hours=i) for i in range(hours)]

    fetch_data.fetch_transform_upload_chunks(site, dts)
    scrape_metadata.scrape_upload_metadata(site, dts)

    for date in set([dt.date() for dt in dts]):
        warehouse.update_dwell_times(site, date)


def run():
    logging.info("Running job...")

    site = get_site(config.get("SITE_NAME"))
    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    status = "success"

    try:

        EXPERIMENT_DT = datetime.now()

        # Step 1: Fetch fresh data, hydrate it, and upload it to the warehouse
        fetch_and_upload_data(site, EXPERIMENT_DT)

        # Step 2: Train models by pulling data from the warehouse and uploading new recommendation objects
        top_articles = warehouse.get_default_recs(site=site)
        save_defaults.save_defaults(top_articles, site, EXPERIMENT_DT.date())

        interactions_data = warehouse.get_dwell_times(site, days=config.get("DAYS_OF_DATA"))

        recommendations = train_model.get_recommendations(
            interactions_data,
            site.config.collaborative_filtering.training_params,
            EXPERIMENT_DT,
        )
        logging.info(f"Successfully trained model on {len(interactions_data)} inputs.")

        save_predictions.save_predictions(site, recommendations)

    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
