import logging
from datetime import datetime
import time
from typing import List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor

from requests.models import Response
from bs4 import BeautifulSoup

from db.mappings.article import Article
from db.helpers import (
    get_articles_by_external_ids,
    refresh_db,
)
from job.steps import warehouse
from sites.site import Site
from sites.helpers import ArticleScrapingError
from lib.metrics import write_metric, Unit
from db.helpers import delete_articles
from db.mappings.base import db_proxy


@refresh_db
def scrape_upload_metadata(site: Site, dts: List[datetime]) -> None:
    """
    Update article metadata for any URLs that were visited during the given dts
    for both Postgres and the Redshift article_cache table
    """
    start_ts = time.time()
    total_scraped = 0
    scraping_errors = 0

    logging.info("Fetching paths to update...")
    df = warehouse.get_paths_to_update(site, dts)

    logging.info(f"Updating {len(df)} paths...")

    # New paths are the ones where the external ID is null
    new_paths = list(df[df["external_id"].isna()]["landing_page_path"])
    new_ext_ids = [e for e in extract_external_ids(site, new_paths) if e]
    # Filter out matching articles in Postgres but not in the cache
    existing_article_ids = set(
        [a.external_id for a in get_articles_by_external_ids(site, new_ext_ids)]
    )
    create_ids = [e for e in new_ext_ids if e not in existing_article_ids]

    logging.info(f"Scraping {len(new_paths)} new paths...")
    n_scraped, n_error = scrape_and_create_articles(
        site,
        [Article(external_id=extid) for extid in create_ids],
    )
    # Paths to refresh are the ones where the external ID is not null
    refresh_ext_ids = set(df["external_id"].dropna()).difference(new_ext_ids)
    logging.info(f"Refreshing {len(refresh_ext_ids)} articles...")
    refresh_articles = get_articles_by_external_ids(site, refresh_ext_ids)
    n_scraped, n_error = scrape_and_update_articles(site, refresh_articles)

    total_scraped += n_scraped
    scraping_errors += n_error

    write_metric("article_scraping_total", total_scraped)
    write_metric("article_scraping_errors", scraping_errors)
    latency = time.time() - start_ts
    write_metric("article_scraping_time", latency, unit=Unit.SECONDS)


def extract_external_id(site: Site, path: str) -> str:
    return site.extract_external_id(path)


def extract_external_ids(site: Site, landing_page_paths: List[str]) -> List[str]:
    """
    :param data_df: DataFrame of activities collected from Snowplow.
        * Requisite fields: "landing_page_path" (str)
    :return: data_df with "external_id" column added
    """
    futures_list = []
    results = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        for path in landing_page_paths:
            future = executor.submit(extract_external_id, site, path=path)
            futures_list.append((path, future))

        for (path, future) in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except:
                pass

    return results


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
