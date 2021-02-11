from typing import List
from datetime import datetime, timezone, timedelta
import logging

import pandas as pd
from scipy.spatial import distance

from db.mappings.model import Type
from db.helpers import create_model, set_current_model
from db.helpers import create_article, update_article, get_article_by_external_id, create_rec
from job import preprocessors
from job.models import ImplicitMF
from sites.sites import Site
from sites.helpers import BadArticleFormatError
from lib.config import config


MAX_RECS = config.get("MAX_RECS")


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


def create_article_to_article_recs(
    model: ImplicitMF, model_id: int, external_ids: List[str], article_df: pd.DataFrame
):
    vector_distances = distance.cdist(model.item_vectors, model.item_vectors, metric="cosine")
    vector_orders = vector_distances.argsort()

    for source_index, ranked_recommendation_indices in enumerate(vector_orders):
        source_external_id = external_ids[source_index]

        # First entry is the article itself, so skip it
        for recommendation_index in ranked_recommendation_indices[1:MAX_RECS]:
            recommended_external_id = external_ids[recommendation_index]

            matching_articles = article_df[article_df["external_id"] == recommended_external_id]
            recommended_article_id = matching_articles["article_id"][0]
            # for distance, smaller values are more highly correlated
            # for score, higher values are more highly correlated
            score = 1 - vector_distances[source_index][recommendation_index]
            # fix case when some scores are negative due to a rounding error
            score = max(score, 0.0)

            rec_id = create_rec(
                source_entity_id=source_external_id,
                model_id=model_id,
                recommended_article_id=recommended_article_id,
                score=score,
            )


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


def calculate_default_recs(ga_df: pd.DataFrame) -> pd.Series:
    TOTAL_VIEWS = 5000
    clean_data = preprocessors.fix_dtypes(ga_df)
    pageviews = clean_data[clean_data["event_action"] == "pageview"]
    # take only the most recent event pageview for each reader
    unique_pageviews = pageviews.loc[
        pageviews.groupby(["client_id", "external_id"]).session_date.idxmax()
    ]
    latest_pageviews = unique_pageviews.nlargest(TOTAL_VIEWS, "session_date")
    top_pageviews = latest_pageviews["external_id"].value_counts().nlargest(MAX_RECS)
    return top_pageviews


def create_default_recs(ga_df: pd.DataFrame, article_df: pd.DataFrame) -> None:
    top_pageviews = calculate_default_recs(ga_df)
    scores = top_pageviews / max(top_pageviews)
    model_id = create_model(type=Type.POPULARITY.value)
    logging.info("Saving default recs to db...")
    for external_id, score in zip(top_pageviews.index, scores):
        matching_articles = article_df[article_df["external_id"] == external_id]
        article_id = matching_articles["article_id"][0]
        rec_id = create_rec(
            source_entity_id="default",
            model_id=model_id,
            recommended_article_id=article_id,
            score=score,
        )

    set_current_model(model_id, Type.POPULARITY.value)
