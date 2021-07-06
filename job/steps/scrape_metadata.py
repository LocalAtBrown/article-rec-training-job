import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List

import pandas as pd
from peewee import IntegrityError

from db.mappings.article import Article
from db.helpers import (
    create_article,
    update_article,
    get_articles_by_external_ids,
    create_rec,
)
from sites.sites import Site
from sites.helpers import ArticleScrapingError
from lib.bucket import save_outputs
from lib.metrics import write_metric, Unit


BACKFILL_ISO_DATE = "2021-03-05"


@save_outputs("article_df.csv")
def scrape_metadata(site: Site, paths: List[int]) -> pd.DataFrame:
    """
    Find articles on news website from list of paths, then associate with corresponding identifiers.

    :param site: Site object enabling retrieval of external ID
    :param paths: Paths on corresponding news website for which to retrieve IDs
    :return: DataFrame of identifiers for collected articles: the path on the website, the external ID,
        and the article ID in the database.
        * Requisite fields: "article_id" (str), "external_id" (str), "landing_page_path" (str)
    """
    start_ts = time.time()
    total_scraped = 0
    scraping_errors = 0

    external_ids = [extract_external_id(site, path) for path in paths]
    articles = get_articles_by_external_ids(external_ids)
    refresh_articles = [a for a in articles if should_refresh(a)]
    found_external_ids = {a.external_id for a in articles}

    for article in refresh_articles:
        try:
            scrape_and_update_article(site=site, article=article)
            total_scraped += 1
        except ArticleScrapingError:
            logging.exception(
                f"Skipping article with external_id: {article.external_id}"
            )
            scraping_errors += 1
    for path, external_id in zip(paths, external_ids):
        if external_id in found_external_ids or external_id is None:
            continue
        try:
            scrape_and_create_article(site=site, path=path, external_id=external_id)
            found_external_ids.add(external_id)
            total_scraped += 1
        except (ArticleScrapingError, IntegrityError):
            logging.exception(f"Skipping article with external_id: {external_id}")
            scraping_errors += 1
    articles = get_articles_by_external_ids(external_ids)
    write_metric("article_scraping_total", total_scraped)
    write_metric("article_scraping_errors", scraping_errors)
    latency = time.time() - start_ts
    write_metric("article_scraping_time", latency, unit=Unit.SECONDS)
    df_data = {
        "article_id": [a.id for a in articles],
        "external_id": [a.external_id for a in articles],
        "published_at": [a.published_at for a in articles],
        "landing_page_path": [a.path for a in articles],
    }
    article_df = pd.DataFrame(df_data).set_index("landing_page_path")
    return article_df


def should_refresh(article: Article) -> bool:
    # refresh metadata without a published time recorded yet
    if not article.published_at:
        return True

    # refresh metadata for articles published within the last day
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    if article.published_at > yesterday:
        return True

    # refresh metadata for articles last updated before the backfill
    backfill_date = datetime.fromisoformat(BACKFILL_ISO_DATE).astimezone(timezone.utc)
    if backfill_date > article.updated_at:
        return True

    return False


def scrape_and_update_article(site: Site, article: Article) -> None:
    article_id = article.id
    external_id = article.external_id
    path = article.path
    metadata = scrape_article_metadata(site, path)
    logging.info(f"Updating article with external_id: {external_id}")
    update_article(article_id, **metadata)


def scrape_and_create_article(site: Site, external_id: int, path: str) -> None:
    metadata = scrape_article_metadata(site, path)
    article_data = {**metadata, "external_id": external_id}
    logging.info(f"Creating article with external_id: {external_id}")
    create_article(**article_data)


def scrape_article_metadata(site: Site, path: str) -> dict:
    return site.scrape_article_metadata(path)


def extract_external_id(site: Site, path: str) -> int:
    return site.extract_external_id(path)
