import time
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Tuple, Optional

from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from peewee import IntegrityError
from requests.models import Response
from bs4 import BeautifulSoup

from db.mappings.article import Article
from db.helpers import (
    create_article,
    update_article,
    get_articles_by_external_ids,
)
from sites.sites import Site
from sites.helpers import ArticleScrapingError
from lib.bucket import save_outputs
from lib.metrics import write_metric, Unit
from db.helpers import delete_articles
from db.mappings.base import db_proxy

BACKFILL_ISO_DATE = "2021-09-08"


def scrape_metadata(site: Site, paths: List[str]) -> pd.DataFrame:
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

    n_scraped, n_error = scrape_and_update_articles(site, refresh_articles)
    total_scraped += n_scraped
    scraping_errors += n_error

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


def scrape_article(site: Site, article: Article) -> Optional[Article]:
    """
    Given a Site and Article object, validate the article and return associated metadata.
    If an error is found, return None
    """
    external_id = article.external_id
    path = article.path
    page, soup, error_msg = validate_article(site, path)
    if error_msg:
        logging.warning(f"Error while validating article {external_id}: '{error_msg}'")
        return None
    metadata = scrape_article_metadata(site, page, soup)
    for key, value in metadata.items():
        if key in Article._meta.fields.keys():
            setattr(article, key, value)
    return article


def scrape_and_update_articles(site: Site, articles: List[Article]) -> Tuple[int, int]:
    """
    Given a site and a list of article objects that need to be updated,
    scrape them and then submit the updated article objects to the database
    if they have a "published_at" field

    Return a tuple (# successful scrapes, # errors)
    """

    logging.info(f"Preparing to refresh {len(articles)} records")

    # Use a concurrent thread pool to do the web scraping, this
    # dramatically increases the speed that we can scrape the articles
    futures_list = []
    results = []
    with ThreadPoolExecutor(max_workers=13) as executor:
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

    # Delete any articles from the DB that had a scraping error
    # These are the external_ids that are not in the result set
    validated_external_ids = {a.external_id for a in results}
    bad_external_ids = [
        a.external_id for a in articles if a.external_id not in validated_external_ids
    ]
    for external_id in bad_external_ids:
        logging.warning(
            f"Deleting article external_id: {external_id} due to scraping error"
        )
    delete_articles(bad_external_ids)

    # Filter for articles that have a published_at date
    to_update = [a for a in results if a.published_at is not None]
    logging.info(f"New publish date for {len(to_update)} records")

    # Use the peewee bulk update method. Update every field that isn't
    # in the RESERVED_FIELDS list
    RESERVED_FIELDS = {"id", "created_at", "updated_at"}
    fields = list(Article._meta.fields.keys() - RESERVED_FIELDS)
    logging.info(f"Bulk updating {len(to_update)} records")
    with db_proxy.atomic():
        Article.bulk_update(to_update, fields, batch_size=50)

    return len(results), len(articles) - len(results)


def scrape_and_create_article(site: Site, external_id: int, path: str) -> None:
    page, soup, error_msg = validate_article(site, path)
    if error_msg:
        logging.warning(
            f"Skipping article with external_id: {external_id}, got error '{error_msg}'"
        )
        return
    metadata = scrape_article_metadata(site, page, soup)
    article_data = {**metadata, "external_id": external_id}
    if article_data.get("published_at") is not None:
        logging.info(f"Creating article with external_id: {external_id}")
        create_article(**article_data)
    else:
        logging.warning(
            f"No publish date, skipping article with external_id: {external_id}"
        )


def validate_article(
    site: Site, path: str
) -> Tuple[Response, BeautifulSoup, Optional[str]]:
    return site.validate_article(path)


def scrape_article_metadata(site: Site, page: Response, soup: BeautifulSoup) -> dict:
    return site.scrape_article_metadata(page, soup)


def extract_external_id(site: Site, path: str) -> int:
    return site.extract_external_id(path)
