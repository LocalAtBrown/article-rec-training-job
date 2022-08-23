import json
import logging
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta

import pandas as pd

from job.helpers.site import get_site
from job.steps.collaborative_filtering import fetch_data as cf_fetch_data
from job.steps.collaborative_filtering import save_defaults as cf_save_defaults
from job.steps.collaborative_filtering import save_predictions as cf_save_predictions
from job.steps.collaborative_filtering import scrape_metadata as cf_scrape_metadata
from job.steps.collaborative_filtering import train_model as cf_train_model
from job.steps.collaborative_filtering import warehouse as cf_warehouse
from job.steps.semantic_similarity import fetch_data as ss_fetch_data
from job.steps.semantic_similarity import generate_embeddings as ss_generate_embeddings
from job.steps.semantic_similarity import generate_recs as ss_generate_recs
from lib.config import config
from lib.metrics import Unit, write_metric
from sites.site import Site


def run():
    site = get_site(config.get("SITE_NAME"))
    logging.info(f"Using site {site.name}")

    EXPERIMENT_DT = datetime.now()

    try:
        # Step 1: Fetch fresh data, hydrate it, and upload it to the warehouse
        fetch_and_upload_data(site, EXPERIMENT_DT)

        # Step 2: Fetch top articles from warehouse and save them as default recs
        # TODO: Post-experiment refactoring: Move modules used by both CF and SS out of collaborative_filtering package
        top_articles = cf_warehouse.get_default_recs(site=site)
        cf_save_defaults.save_defaults(top_articles, site, EXPERIMENT_DT.date())

        # Step 3: Fetch newest interaction data from the warehouse to fetch CF and SS models
        interactions_data = cf_warehouse.get_dwell_times(site, days=config.get("DAYS_OF_DATA"))
        # Texas Tribune article IDs in dev Article table has duplicates that end with ".0"
        # TODO: Remove this bit of code once that's fixed
        if os.environ["STAGE"] != "prod" and site.name == "texas-tribune":
            interactions_data["external_id"] = interactions_data["external_id"].str.replace(".0", "", regex=False)

        # Step 4: Train CF and SS models and save their recommendations
        # TODO: Uncomment the following line before pushing to production
        # run_collaborative_filtering(site, interactions_data, EXPERIMENT_DT)

        # Unlike CF, SS doesn't need user-article activity in interactions_data in order to run,
        # but we're passing interactions_data to run_semantic_similarity because, in this experimentation
        # phase, we only want to run SS on a subset of Texas Tribune articles that CF could "see".
        #
        # There are two reasons for this:
        #
        # - We want to ensure an apples-to-apples comparison between SS and CF. An article recommended via SS
        # has to be, at the very least, considered by the CF model.
        #
        # - Running SS on every article ever published is very memory-intensive, especially in the KNN phase,
        # in which the similarity score array has a dimension of <num_articles> x <num_articles> and therefore
        # grows in memory at a O(n^2) rate. (There are currently 40,000 articles published by the Tribune,
        # whereas during a normal job run, interactions_data has around 7,000 unique articles.)
        run_semantic_similarity(site, interactions_data, EXPERIMENT_DT)

    except Exception as e:
        logging.exception(f"Job failed. Exception encountered: {e}")


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


def run_collaborative_filtering(site: Site, interactions_data: pd.DataFrame, experiment_dt: datetime):
    logging.info("Running Collaborative Filtering...")

    start_ts = time.time()
    status = "success"

    exception = None

    try:
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
        status = "failure"
        exception = e

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

    except Exception as e:
        status = "failure"
        exception = e

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})

    # Raise exception to be handled by upper-level run()
    if exception is not None:
        raise exception


def run_semantic_similarity(site: Site, interactions_data: pd.DataFrame, experiment_dt: datetime):
    logging.info("Running Semantic Similarity...")

    # Currently only accepting Texas Tribune
    if site.name != "texas-tribune":
        logging.info(
            "Can only run semantic similarity job for the Texas Tribune. Specify 'texas-tribune' as SITE_NAME in env.json."
        )
        return

    start_ts = time.time()
    exception = None

    try:
        # Fetch article data from publication API. Preprocess all data so that they're ready for next steps
        article_data = ss_fetch_data.run(site, interactions_data)

        # Generate article-level embeddings
        embeddings = ss_generate_embeddings.run(article_data, config.get("SS_ENCODER"))

        # Create recs from embeddings
        _ = ss_generate_recs.run(embeddings, article_data, config.get("MAX_RECS"))
    except Exception as e:
        exception = e

    latency = time.time() - start_ts
    logging.info(f"Time taken: {timedelta(seconds=latency)}s")

    # Raise exception to be handled by upper-level run()
    if exception is not None:
        raise exception
