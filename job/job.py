import logging
import time
from datetime import datetime

from job.helpers import warehouse
from job.helpers.site import get_site
from job.steps.fetch_and_upload import fetch_and_upload_data
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
        fetch_and_upload_data(site, EXPERIMENT_DT)
        interactions_data = warehouse.get_dwell_times(site, days=config.get("DAYS_OF_DATA"))

        for strategy in site.strategies:
            strategy.prepare(site=site, experiment_time=EXPERIMENT_DT)
            strategy.fetch_data(interactions_data=interactions_data)
            strategy.preprocess_data()
            strategy.generate_embeddings()
            strategy.generate_recommendations()
            strategy.save_recommendations()
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
