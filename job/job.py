import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta

from job.helpers import get_site
from job.steps import (
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
            site.training_params,
            EXPERIMENT_DT,
        )
        logging.info(f"Successfully trained model on {len(interactions_data)} inputs.")

        save_predictions.save_predictions(site, recommendations)

    except Exception as e:
        logging.exception("Job failed")
        status = "failure"

        # TODO: This if-block is only here while we're trying to fix the PyTorch IndexError
        # (https://github.com/LocalAtBrown/article-rec-training-job/pull/140).
        # Once no longer necessary, it'll need to be removed.
        if isinstance(e, IndexError) and "Dimension out of range" in str(e):
            logging.info(
                "IndexError encountered, probably during model fitting. Saving training data and params for future inspection."
            )

            slug = f"{EXPERIMENT_DT.strftime('%Y%m%d-%H%M%S')}_{site.name}"

            # Set up local (container) path to store training data and params
            UPLOADS = "/uploads"
            path_local = f"{UPLOADS}/{slug}"
            if not os.path.isdir(path_local):
                os.makedirs(path_local)

            # Save training data
            interactions_data.to_csv(f"{path_local}/data.csv", index=False)
            # Save training metadata (params & timestamp)
            with open(f"{path_local}/metadata.json", "w") as f:
                json.dump(
                    {
                        "experiment_dt": str(EXPERIMENT_DT),
                        "training_params": site.training_params,
                    },
                    f,
                )

            # S3 remote destination
            path_s3 = f"s3://lnl-sandbox/duy.nguyen/training-job-model-indexerror/{slug}"
            cmd = f"aws s3 cp --recursive {path_local} {path_s3}".split(" ")

            # Copy local folder to S3
            logging.info(" ".join(cmd))
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL).wait()

            # Remove directory when done
            shutil.rmtree(UPLOADS)

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
