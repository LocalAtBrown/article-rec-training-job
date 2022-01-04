import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import numpy as np
from requests.models import Response
from bs4 import BeautifulSoup

from db.mappings.article import Article
from db.helpers import (
    get_articles_by_external_ids,
    refresh_db,
    refresh_db,
)
from sites.site import Site
from sites.helpers import ArticleScrapingError
from lib.bucket import save_outputs
from lib.metrics import write_metric, Unit
from db.helpers import delete_articles
from db.mappings.base import db_proxy

BACKFILL_ISO_DATE = "2021-09-08"


@refresh_db
def scrape_metadata(site: Site, external_ids: List[str]) -> pd.DataFrame:
    """
    Find articles on news website from list of paths, then associate with corresponding identifiers.
    :param site: Site object enabling retrieval of external ID
    :param external_ids: Article id's from external news site
    :return: DataFrame of identifiers for collected articles: the path on the website, the external ID,
        and the article ID in the database.
        * Requisite fields: "article_id" (str), "external_id" (str)
    """
    start_ts = time.time()
    total_scraped = 0
    scraping_errors = 0
    external_ids = set(external_ids)
    articles = get_articles_by_external_ids(site, external_ids)
    refresh_articles = [a for a in articles if should_refresh(a)]
    found_external_ids = {a.external_id for a in articles}

    n_scraped, n_error = scrape_and_update_articles(site, refresh_articles)
    total_scraped += n_scraped
    scraping_errors += n_error

    new_articles = [
        Article(external_id=ext_id)
        for ext_id in external_ids
        if ext_id not in found_external_ids
    ]

    n_scraped, n_error = scrape_and_create_articles(site, new_articles)
    total_scraped += n_scraped
    scraping_errors += n_error

    articles = get_articles_by_external_ids(site, external_ids)

    write_metric("article_scraping_total", total_scraped)
    write_metric("article_scraping_errors", scraping_errors)
    latency = time.time() - start_ts
    write_metric("article_scraping_time", latency, unit=Unit.SECONDS)
    df_data = {
        "article_id": [a.id for a in articles],
        "external_id": [a.external_id for a in articles],
        "published_at": [a.published_at for a in articles],
        "landing_page_path": [a.path for a in articles],
        "site": [a.site for a in articles],
    }
    article_df = pd.DataFrame(df_data)
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


def scrape_article(site: Site, article: Article) -> Article:
    """
    Validate the article and retrieve updated metadata via web scrape.
    Return updated Article object. If an error is found, raises ArticleScrapingError
    """
    external_id = article.external_id
    page, soup, error_msg = validate_article(site, external_id)
    if error_msg:
        logging.warning(f"Error while validating article {external_id}: '{error_msg}'")
        raise ArticleScrapingError(error_msg)
    metadata = scrape_article_metadata(site, page, soup)
    for key, value in metadata.items():
        if key in Article._meta.fields.keys():
            setattr(article, key, value)

    article.site = site.name
    return article


def scrape_articles(
    site: Site, articles: List[Article]
) -> Tuple[List[Article], List[Article]]:
    """
    Use a concurrent thread pool to scrape each of the input articles.
    Return a tuple of:
        list of Article objects with updated, scraped metadata.
        list of Article objects that could not be scraped.
    """
    futures_list = []
    results = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for article in articles:
            future = executor.submit(scrape_article, site, article=article)
            futures_list.append(future)
        for future in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except ArticleScrapingError:
                # The exception is logged in the validate_article method
                pass

    logging.info(f"Successfully scraped {len(results)} records")
    success_ids = {a.external_id for a in results}
    failed_articles = [a for a in articles if a.external_id not in success_ids]
    return results, failed_articles


def scrape_and_create_articles(site: Site, articles: List[Article]) -> Tuple[int, int]:
    """
    Given a Site and list of Article objects, validate the article,
    scrape associated metadata, and save articles to the database
    if they have a published_at field
    Return a tuple (# successful scrapes, # errors)
    """
    results, failed = scrape_articles(site, articles)

    for article in failed:
        logging.warning(f"Skipping article with external_id: {article.external_id}")

    to_create = []
    for article in results:
        if article.published_at is not None:
            to_create.append(article)
        else:
            logging.warning(
                f"No publish date, skipping article with external_id: {article.external_id}"
            )

    logging.info(f"Bulk inserting {len(to_create)} records")
    with db_proxy.atomic():
        Article.bulk_create(to_create, batch_size=50)

    return len(results), len(failed)


def scrape_and_update_articles(site: Site, articles: List[Article]) -> Tuple[int, int]:
    """
    Given a site and a list of article objects that need to be updated,
    scrape them and then submit the updated article objects to the database
    if they have a "published_at" field
    Return a tuple (# successful scrapes, # errors)
    """

    logging.info(f"Preparing to refresh {len(articles)} records")
    results, failed = scrape_articles(site, articles)

    # Delete any articles from the DB that had a scraping error
    for article in failed:
        logging.warning(
            f"Deleting article external_id: {article.external_id} due to scraping error"
        )
    delete_articles([a.external_id for a in failed])

    # Filter for articles that have a published_at date
    to_update = []
    for a in results:
        if a.published_at is not None:
            to_update.append(a)
        else:
            logging.warning(
                f"No publish date, skipping article with external_id: {a.external_id}."
            )

    # Use the peewee bulk update method. Update every field that isn't
    # in the RESERVED_FIELDS list
    RESERVED_FIELDS = {"id", "created_at", "updated_at"}
    fields = list(Article._meta.fields.keys() - RESERVED_FIELDS)

    logging.info(f"Bulk updating {len(to_update)} records")
    with db_proxy.atomic():
        Article.bulk_update(to_update, fields, batch_size=50)

    return len(results), len(failed)


def validate_article(
    site: Site, path: str
) -> Tuple[Response, BeautifulSoup, Optional[str]]:
    return site.validate_article(path)


def scrape_article_metadata(
    site: Site, page: Response, soup: Optional[BeautifulSoup]
) -> dict:
    return site.scrape_article_metadata(page, soup)
