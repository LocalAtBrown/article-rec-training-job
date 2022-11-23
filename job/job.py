import logging
import time
from datetime import datetime

from job.helpers import warehouse
from job.helpers.site import get_site
from job.steps.fetch_and_upload import fetch_and_upload_data
from job.strategies import save_defaults, save_predictions
from job.strategies.collaborative_filtering import train_model
from lib.config import config
from lib.metrics import Unit, write_metric


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

        # Temporary solution: The first position in the array of strategies is currently CFConfig.
        # We will change this line when we implement iteration through different strategies.
        strategy = site.strategies[0]
        recommendations = train_model.get_recommendations(
            interactions_data,
            strategy.training_params,
            EXPERIMENT_DT,
        )
        logging.info(f"Successfully trained model on {len(interactions_data)} inputs.")

        save_predictions.save_predictions(site, recommendations)

    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
