"""
this script allows you to backfill the data warehouse
for the site specified in env.json
"""

import datetime
import logging
import argparse

from job.job import fetch_and_upload_data
from job.helpers import get_site
from db.helpers import db_proxy
from lib.config import config
from sites.site import Site

DELTA = datetime.timedelta(days=1)


def backfill(site: Site, date: datetime.datetime.date, days: int) -> None:
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))
    for _ in range(0, days):
        fetch_and_upload_data(site, date, days=1)
        db_proxy.close()
        db_proxy.connect()
        date += DELTA


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Training Job for rec system")

    parser.add_argument(
        "--start-date",
        dest="start_date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today(),
        help="start date for backfill",
    )
    parser.add_argument(
        "--days", dest="days", type=int, default=7, help="number of days for backfill"
    )
    parser.add_argument(
        "--site",
        dest="site",
        default=config.get("SITE_NAME"),
        help="site name for backfill",
    )

    args = parser.parse_args()
    site = get_site(args.site)
    backfill(site, args.start_date, args.days)
