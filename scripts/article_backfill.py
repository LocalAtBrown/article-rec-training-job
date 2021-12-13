"""
this script allows you to backfill the data warehouse
"""

import datetime
import logging
import argparse

from job.helpers import get_site
from db.helpers import db_proxy
from lib.config import config
from sites.site import Site

DELTA = datetime.timedelta(days=1)


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

        # TODO: for each extracted metadata, create or update the article in our db
        #
        # 1- port this code in steps/scrape_metadata.py to its own func
        # external_ids = set(external_ids)
        # articles = get_articles_by_external_ids(site, external_ids)
        # refresh_articles = [a for a in articles if should_refresh(a)]
        # found_external_ids = {a.external_id for a in articles}
        #
        # 2 - port this code in steps/scrape_metadata.py to its own func
        # for key, value in metadata.items():
        #     if key in Article._meta.fields.keys():
        #         setattr(article, key, value)
        #
        # 3 - port this code in steps/scrape_metadata.py to its own func (same w to_create)
        #
        # to_update = []
        # for a in results:
        #     if a.published_at is not None:
        #         to_update.append(a)
        #     else:
        #         logging.warning(
        #             f"No publish date, skipping article with external_id: {a.external_id}."
        #         )
        # # Use the peewee bulk update method. Update every field that isn't
        # # in the RESERVED_FIELDS list
        # RESERVED_FIELDS = {"id", "created_at", "updated_at"}
        # fields = list(Article._meta.fields.keys() - RESERVED_FIELDS)
        # logging.info(f"Bulk updating {len(to_update)} records")
        # with db_proxy.atomic():
        #     Article.bulk_update(to_update, fields, batch_size=50)

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
