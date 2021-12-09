import datetime
import logging
import argparse

from job.job import fetch_and_upload_data
from job.helpers import get_site
from db.helpers import db_proxy
from lib.config import config

DAY = datetime.timedelta(days=1)


def backfill(date: datetime.datetime.date, days: int):
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))
    site = get_site(config.get("SITE_NAME"))
    for _ in range(0, days):
        fetch_and_upload_data(site, date)
        db_proxy.close()
        db_proxy.connect()
        date += DAY


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Training Job for rec system")

    parser.add_argument(
        "--start-date",
        dest="start_date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today(),
        help="specify if you do not wish upload to data warehouse.",
    )
    parser.add_argument(
        "--days",
        dest="days",
        type=int,
        default=7,
        help="specify if you do not wish upload to data warehouse.",
    )
    args = parser.parse_args()
    backfill(args.start_date, args.days)
