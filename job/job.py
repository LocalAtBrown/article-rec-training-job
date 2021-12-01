import datetime
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
from db.mappings.model import Type
from db.helpers import create_model, set_current_model
from lib.metrics import write_metric, Unit
from lib.config import config
from sites.sites import Sites


def run():
    logging.info("Running job...")

    site = config.site()
    logging.info(f"Using site {site.name}")

    start_ts = time.time()
    status = "success"

    try:
        model_id = create_model(type=Type.ARTICLE.value)
        logging.info(f"Created model with id {model_id}")
        EXPERIMENT_DT = datetime.datetime.now()

        data_df = fetch_data.fetch_data(site, EXPERIMENT_DT)
        data_df = preprocess.extract_external_ids(site, data_df)

        article_df = scrape_metadata.scrape_metadata(
            site, list(data_df.external_id.unique())
        )

        data_df = data_df.join(
            article_df, on="external_id", lsuffix="_original", how="inner"
        )

        warehouse.update_dwell_times(data_df, EXPERIMENT_DT.date(), site)

        data_df = preprocess.filter_activities(data_df)
        data_df = preprocess.filter_articles(data_df)

        article_df = article_df.reset_index()
        save_defaults.save_defaults(data_df, article_df)

        # Hyperparameters derived using optimize_ga_pipeline.ipynb notebook in google-analytics-exploration
        formatted_df = preprocess.model_preprocessing(
            data_df, date_list=[EXPERIMENT_DT.date()], half_life=59.631698
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
        set_current_model(model_id, site.name, Type.ARTICLE.value)

        delete_old_models.delete_old_models()
    except Exception:
        logging.exception("Job failed")
        status = "failure"

    latency = time.time() - start_ts
    write_metric("job_time", latency, unit=Unit.SECONDS, tags={"status": status})
