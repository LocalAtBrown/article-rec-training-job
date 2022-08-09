import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta

from job.helpers import get_site
from job.steps.collaborative_filtering import fetch_data as cf_fetch_data
from job.steps.collaborative_filtering import save_defaults as cf_save_defaults
from job.steps.collaborative_filtering import save_predictions as cf_save_predictions
from job.steps.collaborative_filtering import scrape_metadata as cf_scrape_metadata
from job.steps.collaborative_filtering import train_model as cf_train_model
from job.steps.collaborative_filtering import warehouse as cf_warehouse
from job.steps.semantic_similarity import fetch_data as ss_fetch_data
from lib.config import config
from lib.metrics import Unit, write_metric
from sites.site import Site


def run():
    EXPERIMENT_DT = datetime.now()
    # run_collaborative_filtering(EXPERIMENT_DT)
    run_semantic_similarity(EXPERIMENT_DT)


def fetch_and_upload_data(site: Site, dt: datetime, hours=config.get("HOURS_OF_DATA")):
    """
    1. Upload transformed events data to Redshift
    2. Update article metadata
    3. Update dwell times table
    """
    dts = [dt - timedelta(hours=i) for i in range(hours)]

    cf_fetch_data.fetch_transform_upload_chunks(site, dts)
    cf_scrape_metadata.scrape_upload_metadata(site, dts)

    for date in set([dt.date() for dt in dts]):
        cf_warehouse.update_dwell_times(site, date)


def run_collaborative_filtering(experiment_dt: datetime):
    logging.info("Running job: Collaborative Filtering...")

    site = get_site(config.get("SITE_NAME"))
    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    status = "success"

    try:

        # Step 1: Fetch fresh data, hydrate it, and upload it to the warehouse
        fetch_and_upload_data(site, experiment_dt)

        # Step 2: Train models by pulling data from the warehouse and uploading new recommendation objects
        top_articles = cf_warehouse.get_default_recs(site=site)
        cf_save_defaults.save_defaults(top_articles, site, experiment_dt.date())

        interactions_data = cf_warehouse.get_dwell_times(site, days=config.get("DAYS_OF_DATA"))

        recommendations = cf_train_model.get_recommendations(
            interactions_data,
            site.training_params,
            experiment_dt,
        )
        logging.info(f"Successfully trained model on {len(interactions_data)} inputs.")

        cf_save_predictions.save_predictions(site, recommendations)

    # TODO: This execpt-block is only here while we're trying to fix the PyTorch IndexError
    # (https://github.com/LocalAtBrown/article-rec-training-job/pull/140).
    # Once no longer necessary, it'll need to be removed.
    except IndexError as e:
        logging.exception("Job failed")
        status = "failure"

        if "Dimension out of range" in str(e):
            logging.info(
                "IndexError encountered, probably during model fitting. Saving training data and params for future inspection."
            )

            slug = f"{experiment_dt.strftime('%Y%m%d-%H%M%S')}_{site.name}"

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
                        "experiment_dt": str(experiment_dt),
                        "training_params": site.training_params,
                    },
                    f,
                )

            # S3 remote destination. TODO: Parametrize if we end up keeping this check.
            path_s3 = f"s3://lnl-sandbox/duy.nguyen/training-job-model-indexerror/{slug}"
            cmd = f"aws s3 cp --recursive {path_local} {path_s3}".split(" ")

            # Copy local folder to S3
            logging.info(" ".join(cmd))
            subprocess.Popen(cmd, stdout=subprocess.DEVNULL).wait()

            # Remove directory when done
            shutil.rmtree(UPLOADS)

    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})


# Added to accommodate SS
def run_semantic_similarity(experiment_dt: datetime):
    logging.info("Running job: Semantic Similarity...")

    site = get_site(config.get("SITE_NAME"))

    # Currently only accepting Texas Tribune
    if site.name != "texas-tribune":
        logging.info(
            "Can only run semantic similarity job for the Texas Tribune. Specify 'texas-tribune' as SITE_NAME in env.json."
        )
        return

    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    # status = "success"

    try:
        # If implement after experimentation phase, move to sites/texas_tribune.py
        # (or some post-refactoring equivalent) as a constant
        SS_BULK_FETCH_START = datetime(1999, 3, 15)
        _ = ss_fetch_data.run(site, SS_BULK_FETCH_START, experiment_dt)
    except Exception as e:
        logging.exception(f"Job failed. Exception encountered: {e}")
        # status = "failure"

    latency = time.time() - start_ts
    logging.info(f"Time taken: {timedelta(seconds=latency)}s")
