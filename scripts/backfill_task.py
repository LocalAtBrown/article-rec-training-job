"""
this script allows you to backfill the data warehouse
"""
import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import datetime
import logging

from job.helpers import get_site
from job.job import fetch_and_upload_data
from lib.config import config
from sites.site import Site

DELTA = datetime.timedelta(days=1)


def backfill(site: Site, dt: datetime.datetime, days: int) -> None:
    site = Site(
        name=site.name,
        fields=site.fields,
        hours_of_data=24,
        training_params=site.training_params,
        scrape_config=site.scrape_config,
        transform_raw_data=site.transform_raw_data,
        extract_external_id=site.extract_external_id,
        scrape_article_metadata=site.scrape_article_metadata,
        fetch_article=site.fetch_article,
        bulk_fetch=site.bulk_fetch,
        popularity_window=site.popularity_window,
        max_article_age=site.max_article_age,
    )

    for _ in range(0, days):
        fetch_and_upload_data(site, dt)
        dt -= DELTA


if __name__ == "__main__":
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))
    parser = argparse.ArgumentParser(description="Training Job for rec system")

    parser.add_argument(
        "--start-date",
        dest="start_date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today(),
        help="YYYY-MM-DD start date for backfill (default: today)",
    )
    parser.add_argument(
        "--days",
        dest="days",
        type=int,
        default=7,
        help="integer: number of days for backfill (default: 7)",
    )
    parser.add_argument(
        "--site",
        dest="site",
        default=config.get("SITE_NAME"),
        help=f"site name for backfill (default: {config.get('SITE_NAME')})",
    )

    args = parser.parse_args()
    site = get_site(args.site)
    start_dt = datetime.datetime.combine(args.start_date, datetime.time.min)
    # Start the backfill at midnight of the given date
    backfill(site, start_dt, args.days)
