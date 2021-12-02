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
from db.helpers import create_model, set_current_model, db_proxy
from lib.metrics import write_metric, Unit
from lib.config import config
from sites.sites import Sites


def run(date):
    logging.info(f"{date} Running job...")
    db_proxy.close()
    db_proxy.connect()
    site = config.site()
    logging.info(f"Using site {site.name}")

    data_df = fetch_data.fetch_data(site, date, days=1)
    data_df = preprocess.extract_external_ids(site, data_df)

    article_df = scrape_metadata.scrape_metadata(
        site, list(data_df.external_id.unique())
    )

    data_df = data_df.join(
        article_df, on="external_id", lsuffix="_original", how="inner"
    )

    warehouse.update_dwell_times(data_df, date, site)


def run_all():
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))
    today = datetime.datetime(2021, 11, 29)
    date = datetime.datetime(2021, 11, 9)
    DAY = datetime.timedelta(days=1)
    while date < today:
        run(date.date())
        date += DAY
