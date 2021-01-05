from datetime import datetime, timezone, timedelta
import logging
import pandas as pd

from db.helpers import create_article, update_article, get_article_by_external_id
from job import preprocessors
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


def find_or_create_articles(site: Site, paths: list) -> pd.DataFrame:
    articles = []

    logging.info(f"Finding or creating articles for {len(paths)} paths")

    for path in paths:
        external_id = extract_external_id(site, path)
        if external_id:
            article_id = find_or_create_article(site, external_id, path)
            articles.append({
                'article_id': article_id,
                'external_id': external_id,
                'page_path': path
            })

    article_df = pd.DataFrame(articles).set_index('page_path')

    return article_df


def format_ga(
        ga_df: pd.DataFrame,
        date_list: list = [],
        external_id_col: str = 'external_id',
        half_life: float = 10.0
    ) -> pd.DataFrame:
    clean_df = preprocessors.fix_dtypes(ga_df)
    sorted_df = preprocessors.time_activities(clean_df)
    filtered_df = preprocessors.filter_activities(sorted_df)
    time_df = preprocessors.aggregate_time(
        filtered_df,
        date_list=date_list,
        external_id_col=external_id_col
    )
    exp_time_df = preprocessors.time_decay(
        time_df, half_life=half_life
    )

    return exp_time_df

