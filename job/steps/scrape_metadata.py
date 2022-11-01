import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import List, Tuple, Union

from db.helpers import (
    delete_articles,
    get_articles_by_external_ids,
    get_existing_external_ids,
    refresh_db,
)
from db.mappings.article import Article
from db.mappings.base import db_proxy
from db.mappings.path import Path
from job.steps import warehouse
from lib.metrics import Unit, write_metric

from sites.article_scraping_error import (
    ArticleScrapingError,
    ScrapeFailure)
from sites.site import Site

EXCLUDE_FAILURE_TYPES = {
    ScrapeFailure.NO_EXTERNAL_ID,
    ScrapeFailure.FAILED_SITE_VALIDATION,
}


@refresh_db
def scrape_upload_metadata(site: Site, dts: List[datetime]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Update article metadata for any URLs that were visited during the given dts.

    URLs from the events table are joined with the Postgres articles database
    URLs with no associated article ID are "new urls" that we need to create.
    URLs with an associated article ID are "refresh urls" that we need to update.
    Update the Postgres path->externalID table with any new paths
    """
    start_time = time.time()

    logging.info("Fetching paths to update...")
    df = warehouse.get_paths_to_update(site, dts)

    # New paths are the ones where the external ID is null
    new_paths = list(df[df["external_id"].isna()]["landing_page_path"])
    create_results, create_errors = scrape_and_create_articles(
        site,
        new_paths,
    )

    # Paths to refresh are the ones where the external ID is not null
    refresh_ext_ids = list(df["external_id"].dropna().unique())
    update_results, update_errors = scrape_and_update_articles(site, refresh_ext_ids)

    all_errors = create_errors + update_errors

    update_path_cache(site, create_results, all_errors)

    write_metric("article_scraping_total", len(create_results) + len(update_results))
    write_metric("article_scraping_creates", len(create_results))
    write_metric("article_scraping_updates", len(update_results))

    latency = time.time() - start_time
    write_metric("article_scraping_time", latency, unit=Unit.SECONDS)
    return (
        create_results + update_results,
        all_errors,
    )


def update_path_cache(site: Site, create_results: List[Article], errors: List[ArticleScrapingError]):
    """
    Given the created articles and all the scraping errors, write new entries to the path->external ID table
    """
    to_create = []
    for e in errors:
        if e.error_type in EXCLUDE_FAILURE_TYPES:
            to_create.append(
                Path(
                    path=e.path,
                    external_id=None,
                    exclude_reason=e.error_type.value,
                    site=site.name,
                )
            )
        elif e.error_type == ScrapeFailure.DUPLICATE_PATH:
            to_create.append(
                Path(
                    path=e.path,
                    external_id=e.external_id,
                    exclude_reason=None,
                    site=site.name,
                )
            )

    num_unhandled_errors = len(errors) - len(to_create)

    for c in create_results:
        to_create.append(
            Path(
                path=c.path,
                external_id=c.external_id,
                exclude_reason=None,
                site=site.name,
            )
        )

    with db_proxy.atomic():
        Path.bulk_create(to_create, batch_size=50)

    write_metric("article_scraping_errors", num_unhandled_errors)
    write_metric("article_scraping_paths_written", len(to_create))


def extract_external_id(site: Site, path: str) -> str:
    return site.extract_external_id(path)


def extract_external_ids(site: Site, landing_page_paths: List[str]) -> List[Union[str, ArticleScrapingError]]:
    """
    Attempts to extract externalIDs from a list of URLs
    :param landing_page_paths: List of unique landing page paths
    :return: list of "external_id" in the same order as the input, or ArticleScrapingError
        if the extraction failed
    """
    futures_list = []
    results: List[Union[str, ArticleScrapingError]] = []

    with ThreadPoolExecutor(max_workers=site.scrape_config["concurrent_requests"]) as executor:
        for path in landing_page_paths:
            future = executor.submit(extract_external_id, site, path=path)
            futures_list.append((path, future))

        for (path, future) in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except ArticleScrapingError as e:
                logging.warning(
                    "Failed to scrape external ID. "
                    + f"Path: {e.path}. "
                    + f"Type: {e.error_type}. "
                    + f"Message: {e.msg}"
                )
                results.append(e)
    return results


def scrape_article(site: Site, article: Union[Article, ArticleScrapingError]) -> Article:
    """
    Fetch the article and retrieve updated metadata. If the input is an ArticleScrapingError,
    just return it

    :param article: Article object containing external_id, or ArticleScrapingError
    :return: List of updated Article objects, or ArticleScrapingError
    """
    if isinstance(article, ArticleScrapingError):
        raise article
    res = site.fetch_article(article.external_id, article.path)
    metadata = site.scrape_article_metadata(res, article.external_id, article.path)
    for key, value in metadata.items():
        if key in Article._meta.fields.keys():
            setattr(article, key, value)

    article.site = site.name
    return article


def scrape_articles(site: Site, articles: List[Article]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Use a concurrent thread pool to scrape each of the input articles.
    Return a list of Article objects with updated, scraped metadata
        and ArticleScrapingError if the article could not be scraped.
    """
    futures_list = []
    results: List[Article] = []
    errors: List[ArticleScrapingError] = []
    with ThreadPoolExecutor(max_workers=site.scrape_config["concurrent_requests"]) as executor:
        for article in articles:
            future = executor.submit(scrape_article, site, article=article)
            futures_list.append(future)
        for future in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except ArticleScrapingError as e:
                logging.warning(
                    f"Failed to scrape article!! " + f"Path: {e.path}. " + f"Type: {e.error_type}. " + f"Message: {e.msg}"
                )
                errors.append(e)

    logging.info(f"Scraped {len(results)} records")
    return results, errors


def new_articles_from_paths(site: Site, paths: List[str]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Given a list of path strings, return two lists. the first is a list of valid Article objects
    that need to be scraped and written to the DB. the second is a list of ArticleScrapingErrors
    """
    # First, extract external IDs from the paths
    external_ids = extract_external_ids(site, paths)
    existing_external_ids = set(get_existing_external_ids(site, [e for e in external_ids if isinstance(e, str)]))

    new_articles = []
    errors = []
    for path, ext_id in zip(paths, external_ids):
        if isinstance(ext_id, ArticleScrapingError):
            errors.append(ext_id)
        elif ext_id in existing_external_ids:
            # We found a new path that maps to an existing external ID
            errors.append(ArticleScrapingError(ScrapeFailure.DUPLICATE_PATH, path, ext_id))
        else:
            new_articles.append(Article(external_id=ext_id, path=path))
            existing_external_ids.add(ext_id)

    return new_articles, errors


def scrape_and_create_articles(site: Site, paths: List[str]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Given a Site and list of paths (or external ID scrape errors),
    fetch the article, scrape associated metadata, and save articles and paths to the database

    Return a list of created article objects corresponding to the input IDs.
    ArticleScrapeErrors are given for articles that failed to be created
    """
    logging.info(f"Inspecting {len(paths)} new paths")
    articles, errors = new_articles_from_paths(site, paths)
    results, scrape_errors = scrape_articles(site, articles)
    errors = errors + scrape_errors

    to_create = []
    for a in results:
        if a.published_at is not None:
            to_create.append(a)
        else:
            errors.append(ArticleScrapingError(ScrapeFailure.NO_PUBLISH_DATE, a.path, a.external_id))

    logging.info(f"Bulk inserting {len(to_create)} records")
    with db_proxy.atomic():
        Article.bulk_create(to_create, batch_size=50)

    return results, errors


def scrape_and_update_articles(site: Site, external_ids: List[str]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Given a site and a list of article objects that need to be updated,
    scrape them and then submit the updated article objects to the database
    if they have a "published_at" field
    Return a tuple (# successful scrapes, # errors)
    """

    logging.info(f"Updating {len(external_ids)} records")

    articles = get_articles_by_external_ids(site, external_ids)

    results, errors = scrape_articles(site, articles)

    # Delete any articles from the DB that had a scraping error
    delete_articles([e.external_id for e in errors])

    # Filter for articles that have a published_at date
    to_update = []
    for a in results:
        if a.published_at is not None:
            to_update.append(a)
        else:
            errors.append(ArticleScrapingError(ScrapeFailure.NO_PUBLISH_DATE, a.path, a.external_id))

    # Use the peewee bulk update method. Update every field that isn't
    # in the RESERVED_FIELDS list
    RESERVED_FIELDS = {"id", "created_at"}
    fields = list(Article._meta.fields.keys() - RESERVED_FIELDS)

    logging.info(f"Bulk updating {len(to_update)} records")
    with db_proxy.atomic():
        Article.bulk_update(to_update, fields, batch_size=50)

    return results, errors
