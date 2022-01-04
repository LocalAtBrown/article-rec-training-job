"""
this script allows you to backfill our article table for a given site
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
from db.helpers import update_article, create_article

DELTA = datetime.timedelta(days=1)


def update_or_create(site: Site, metadata: Dict[str, Any]):
    metadata["site"] = site.name
    try:
        article = Article.get(
            (Article.site == site.name)
            & (Article.external_id == metadata["external_id"])
        )
        logging.info(f'Updating article with external_id: {metadata["external_id"]}')
        update_article(article.id, **metadata)
    except Article.DoesNotExist:
        logging.info(f'Creating article with external_id: {metadata["external_id"]}')
        article_id = create_article(**metadata)


def backfill(site: Site, start_date: datetime.datetime.date, days: int) -> None:
    log_level = config.get("LOG_LEVEL")
    logging.getLogger().setLevel(logging.getLevelName(log_level))

    date = start_date
    for _ in range(0, days):
        try:
            res = site.bulk_fetch(date, date + DELTA)
        except NotImplementedError:
            logging.error(f"`bulk_fetch` not implemented for site: {site.name}")
            return

        logging.info(f"Updating or creating {len(res)} articles...")
        for metadata in res:
            update_or_create(site, metadata)

        db_proxy.close()
        db_proxy.connect()
        date -= DELTA


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
