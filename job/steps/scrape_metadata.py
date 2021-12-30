import logging
from datetime import datetime
import time
from typing import List, Tuple, Union
from concurrent.futures import ThreadPoolExecutor


from db.mappings.article import Article
from db.mappings.path import Path
from db.helpers import (
    get_articles_by_external_ids,
    refresh_db,
    get_existing_external_ids,
)
from job.steps import warehouse
from sites.site import Site
from sites.helpers import ArticleScrapingError, ScrapeFailure
from lib.metrics import write_metric, Unit
from db.helpers import delete_articles
from db.mappings.base import db_proxy

SAFE_FAILURE_TYPES = {
    ScrapeFailure.NO_EXTERNAL_ID,
    ScrapeFailure.EXCLUDE_TAG,
}


@refresh_db
def scrape_upload_metadata(site: Site, dts: List[datetime]) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Update article metadata for any URLs that were visited during the given dts.

    URLs from the events table are joined with the Postgres articles database
    URLs with no associated article ID are "new urls" that we need to create.
    URLs with an associated article ID are "refresh urls" that we need to update.
    When multiple URLs resolve to the same article ID, we update Postgres with the new URL.
    """
    start_time = time.time()

    logging.info("Fetching paths to update...")
    df = warehouse.get_paths_to_update(site, dts)

    logging.info(f"Updating {len(df)} paths...")

    # New paths are the ones where the external ID is null
    new_paths = list(df[df["external_id"].isna()]["landing_page_path"])
    create_results, create_errors = scrape_and_create_articles(
        site,
        new_paths,
    )

    # Paths to refresh are the ones where the external ID is not null
    refresh_ext_ids = list(df["external_id"].dropna().unique())
    update_results, update_errors = scrape_and_update_articles(site, refresh_ext_ids)

    errors = create_errors + update_errors
    safe_errors = [e for e in errors if e.error_type in SAFE_FAILURE_TYPES]
    unsafe_errors = [e for e in errors if e.error_type not in SAFE_FAILURE_TYPES]

    to_create = []
    for e in safe_errors:
        to_create.append(Path(path=e.path, exclude_reason=e.error_type.value, site=site.name))

    logging.info(f"Bulk creating {len(safe_errors)} exclude URLs")
    with db_proxy.atomic():
        Path.bulk_create(to_create, batch_size=50)

    write_metric("article_scraping_total", len(create_results) + len(update_results))

    write_metric("article_scraping_errors", len(unsafe_errors))
    latency = time.time() - start_time
    write_metric("article_scraping_time", latency, unit=Unit.SECONDS)
    return create_results + update_results, errors,


def extract_external_id(site: Site, path: str) -> str:
    return site.extract_external_id(path)


def extract_external_ids(
    site: Site, landing_page_paths: List[str]
) -> List[Union[str, ArticleScrapingError]]:
    """
    Attempts to extract externalIDs from a list of URLs
    :param landing_page_paths: List of unique landing page paths
    :return: list of "external_id" in the same order as the input, or ArticleScrapingError
        if the extraction failed
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
            except ArticleScrapingError as e:
                logging.warning(
                    f"Failed to scrape External ID for path {e.path} {e.msg}"
                )
                results.append(e)
    return results


def scrape_article(
    site: Site, article: Union[Article, ArticleScrapingError]
) -> Union[Article, ArticleScrapingError]:
    """
    Fetch the article and retrieve updated metadata. If the input is an ArticleScrapingError,
    just return it

    :param article: Article object containing external_id, or ArticleScrapingError
    :return: List of updated Article objects, or ArticleScrapingError
    """
    if isinstance(article, ArticleScrapingError):
        return article
    res = site.fetch_article(article.external_id, article.path)
    metadata = site.scrape_article_metadata(res, article.external_id, article.path)
    for key, value in metadata.items():
        if key in Article._meta.fields.keys():
            setattr(article, key, value)

    article.site = site.name
    return article


def scrape_articles(
    site: Site, articles: List[Article]
) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Use a concurrent thread pool to scrape each of the input articles.
    Return a list of Article objects with updated, scraped metadata
        and ArticleScrapingError if the article could not be scraped.
    """
    futures_list = []
    results = []
    errors = []
    with ThreadPoolExecutor(max_workers=20) as executor:
        for article in articles:
            future = executor.submit(scrape_article, site, article=article)
            futures_list.append(future)
        for future in futures_list:
            try:
                result = future.result(timeout=60)
                results.append(result)
            except ArticleScrapingError as e:
                logging.warning(
                    f"ArticleScrapingError! {e.error_type}, {e.path}, {e.msg}"
                )
                errors.append(e)

    logging.info(f"Scraped {len(results)} records")
    return results, errors


def new_articles_from_paths(
    site: Site, paths: List[str]
) -> Tuple[List[Article], List[ArticleScrapingError]]:
    # First, throw out any that are excluded on the path list
    exclude_urls = Path.select().where(
        (Path.site == site.name) & (Path.path.in_(paths) & (Path.exclude_reason is not None))
    )
    exclude_paths = set([p.path for p in exclude_urls])
    logging.info(f"Skipping {len(exclude_paths)} paths marked as ignore")
    paths = [p for p in paths if p not in exclude_paths]

    # First, extract external IDs from the paths
    external_ids = extract_external_ids(site, paths)

    new_articles = []
    errors = []
    for path, ext_id in zip(paths, external_ids):
        if isinstance(ext_id, ArticleScrapingError):
            errors.append(ext_id)
        else:
            new_articles.append(Article(external_id=ext_id, path=path))

    # Drop any articles that actually already exist
    # Sometimes multiple paths resolve to the same external ID,
    # so the db could be missing a path whose external ID already exists
    existing_external_ids = set(get_existing_external_ids(site, [a.external_id for a in new_articles]))

    logging.info(f"Not scraping {len(existing_external_ids)} paths from pre-existing records")
    new_path_entries = [Path(external_id=a.external_id, site=site.name, path=a.path)
                        for a in new_articles if a.external_id in existing_external_ids]
    logging.info(f"Adding {len(new_path_entries)} new paths to pre-existing records")
    with db_proxy.atomic():
        Path.bulk_create(new_path_entries, batch_size=50)
    new_articles = [a for a in new_articles if a.external_id not in existing_external_ids]
    return new_articles, errors


def scrape_and_create_articles(
    site: Site, paths: List[str]
) -> Tuple[List[Article], List[ArticleScrapingError]]:
    """
    Given a Site and list of paths (or external ID scrape errors),
    fetch the article, scrape associated metadata, and save articles to the database

    Return a list of created article objects corresponding to the input IDs.
    ArticleScrapeErrors are given for articles that failed to be created
    """
    logging.info(f"Creating {len(paths)} new articles")
    articles, errors = new_articles_from_paths(site, paths)
    results, scrape_errors = scrape_articles(site, articles)
    errors = errors + scrape_errors

    to_create = []
    for a in results:
        if a.published_at is not None:
            to_create.append(a)
        else:
            errors.append(ArticleScrapingError(
                ScrapeFailure.NO_PUBLISH_DATE, a.path, a.external_id
            ))

    logging.info(f"Bulk inserting {len(to_create)} records")
    with db_proxy.atomic():
        Article.bulk_create(to_create, batch_size=50)

    return results, errors


def scrape_and_update_articles(
    site: Site, external_ids: List[str]
) -> Tuple[List[Article], List[ArticleScrapingError]]:
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
