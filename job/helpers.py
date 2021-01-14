from datetime import datetime, timezone, timedelta
import logging
import pandas as pd

from db.helpers import create_article, update_article, get_article_by_external_id
from job import preprocessors
from sites.sites import Site
from sites.helpers import BadArticleFormatError


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
    """
    Find articles on news website from list of paths, then associate with corresponding identifiers.

    :param site: Site object enabling retrieval of external ID
    :param paths: Paths on corresponding news website for which to retrieve IDs
    :return: DataFrame of identifiers for collected articles: the path on the website, the external ID,
        and the article ID in the database.
        * Requisite fields: "article_id" (str), "external_id" (str), "page_path" (str)
    """
    articles = []

    logging.info(f"Finding or creating articles for {len(paths)} paths")

    for path in paths:
        external_id = extract_external_id(site, path)
        if external_id:
            try:
                article_id = find_or_create_article(site, external_id, path)
            except BadArticleFormatError:
                logging.exception(f"Skipping article with external_id: {external_id}")
                continue
            articles.append(
                {"article_id": article_id, "external_id": external_id, "page_path": path}
            )

    article_df = pd.DataFrame(articles).set_index("page_path")

    return article_df


def format_ga(
    ga_df: pd.DataFrame,
    date_list: list = [],
    external_id_col: str = "external_id",
    half_life: float = 10.0,
) -> pd.DataFrame:
    """
    Format clickstream Google Analytics data into user-item matrix for training.

    :param ga_df: DataFrame of activities collected from Google Analytics using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :param date_list: List of datetimes to forcefully include in all aggregates
    :param external_id_col: Name of column being used to denote articles
    :param half_life: Desired half life of time spent in days
    :return: DataFrame with one row for each user at each date of interest, and one column for each article
    """
    clean_df = preprocessors.fix_dtypes(ga_df)
    sorted_df = preprocessors.time_activities(clean_df)
    filtered_df = preprocessors.filter_activities(sorted_df)
    time_df = preprocessors.aggregate_time(
        filtered_df, date_list=date_list, external_id_col=external_id_col
    )
    exp_time_df = preprocessors.time_decay(time_df, half_life=half_life)

    return exp_time_df
