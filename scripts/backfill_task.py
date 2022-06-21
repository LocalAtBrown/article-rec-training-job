"""
this script allows you to backfill the data warehouse
"""

import argparse
import datetime
import logging

from job.helpers import get_site
from job.job import fetch_and_upload_data
from lib.config import config
from sites.site import Site

DELTA = datetime.timedelta(days=1)


def backfill(site: Site, dt: datetime.datetime, days: int) -> None:

    for _ in range(0, days):
        fetch_and_upload_data(site, dt, hours=24)
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
