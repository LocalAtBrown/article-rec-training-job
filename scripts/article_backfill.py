"""
this script allows you to backfill the data warehouse
"""

import datetime
import logging
import argparse
from typing import Dict, Any

from job.helpers import get_site
from db.helpers import db_proxy
from lib.config import config
from sites.site import Site
from db.mappings.article import Article

DELTA = datetime.timedelta(days=1)


def update_or_create(site: Site, metadata: Dict[str, Any]):
    metadata["site"] = site.name
    # TODO: move update or create to db/helpers.py
    try:
        article = Article.get(
            (Article.site == site.name)
            & (Article.external_id == metadata["external_id"])
        )
        # TODO: update article here
    except Article.DoesNotExist:
        Article.create(**metadata)


def backfill(site: Site, start_date: datetime.datetime.date, days: int) -> None:
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    for _ in range(0, days):
        end_date = start_date + DELTA
        try:
            res = site.bulk_fetch(start_date, end_date)
        except NotImplementedError:
            logging.error(f"`bulk_fetch` not implemented for site: {site.name}")
            return

        for metadata in res:
            update_or_create(metadata)

        db_proxy.close()
        db_proxy.connect()
        start_date = end_date


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Training Job for rec system")

    parser.add_argument(
        "--start-date",
        dest="start_date",
        type=datetime.date.fromisoformat,
        default=datetime.date.today() - datetime.timedelta(days=7),
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
