"""
this script allows you to backfill our article table for a given site
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import datetime
import logging
from typing import Any, Dict

from db.helpers import create_article, db_proxy, update_article
from db.mappings.article import Article
from job.helpers import get_site
from lib.config import config
from sites.helpers import ArticleBulkScrapingError
from sites.site import Site

DELTA = datetime.timedelta(days=1)


def update_or_create(site: Site, metadata: Dict[str, Any]):
    metadata["site"] = site.name
    try:
        article = Article.get((Article.site == site.name) & (Article.external_id == metadata["external_id"]))
        logging.info(f'Updating article with external_id: {metadata["external_id"]}')
        update_article(article.id, **metadata)
    except Article.DoesNotExist:
        logging.info(f'Creating article with external_id: {metadata["external_id"]}')
        create_article(**metadata)


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
        except ArticleBulkScrapingError as e:
            logging.info(
                f"Backfill failed for site {site.name}'s articles between {start_date} and {end_date}. Try again."
            )
            logging.exception(e)
            # Proceed to other days
            continue

        logging.info(f"Updating or creating {len(res)} articles...")
        for metadata in res:
            update_or_create(site, metadata)

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
    parser.add_argument("--days", dest="days", type=int, default=7, help="number of days for backfill")
    parser.add_argument(
        "--site",
        dest="site",
        default=config.get("SITE_NAME"),
        help="site name for backfill",
    )

    args = parser.parse_args()
    site = get_site(args.site)
    backfill(site, args.start_date, args.days)
