from datetime import datetime, timezone, timedelta
import logging
import random

from db.helpers import create_article, update_article, get_article_by_external_id
from sites.sites import Site


def should_refresh(publish_ts: str) -> bool:
    # refresh metadata without a published time recorded yet
    if not publish_ts:
        return True

    # refresh metadata for articles published within the last day
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    if datetime.fromisoformat(publish_ts) > yesterday:
        return True

    return False


def find_or_create_article(site: Site, external_id: int, path: str) -> int:
    logging.info(f"Fetching article with external_id: {external_id}")
    article = get_article_by_external_id(external_id)
    if article:
        if should_refresh(article["published_at"]):
            metadata = scrape_article_metadata(site, path)
            logging.info(f"Updating article with external_id: {external_id}")
            update_article(article["id"], **metadata)
        return article["id"]

    metadata = scrape_article_metadata(site, path)
    article_data = {**metadata, "external_id": external_id}
    logging.info(f"Creating article with external_id: {external_id}")
    article_id = create_article(**article_data)

    return article_id


def scrape_article_metadata(site: Site, path: str) -> dict:
    return site.scrape_article_metadata(path)


def extract_external_id(site: Site, path: str) -> int:
    return site.extract_external_id(path)


def find_or_create_articles(site: Site, paths: list) -> dict:
    article_dict = {}

    logging.info(f"Finding or creating articles for {len(paths)} paths")

    for path in random.sample(paths, 10):  # TODO test with ten paths
        external_id = extract_external_id(site, path)
        if external_id:
            article_id = find_or_create_article(site, external_id, path)
            article_dict[external_id] = article_id

    return article_dict
