import time
import logging
import numpy as np
import pandas as pd

from datetime import datetime, timezone, timedelta
from scipy.spatial import distance
from typing import List
from peewee import IntegrityError

from db.mappings.model import Type
from db.mappings.article import Article
from db.helpers import create_model, set_current_model
from db.helpers import (
    create_article,
    update_article,
    get_articles_by_external_ids,
    create_rec,
)
from job import preprocessors
from job.models import ImplicitMF
from sites.sites import Site
from sites.helpers import BadArticleFormatError
from lib.config import config
from lib.metrics import write_metric, Unit


MAX_RECS = config.get("MAX_RECS")
BACKFILL_ISO_DATE = "2021-03-05"


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


def find_or_create_articles(site: Site, paths: List[int]) -> pd.DataFrame:
    """
    Find articles on news website from list of paths, then associate with corresponding identifiers.

    :param site: Site object enabling retrieval of external ID
    :param paths: Paths on corresponding news website for which to retrieve IDs
    :return: DataFrame of identifiers for collected articles: the path on the website, the external ID,
        and the article ID in the database.
        * Requisite fields:
            "article_id" (str),
            "external_id" (str),
            "landing_page_path" (str),
            "published_at" (datetime)
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
        except BadArticleFormatError:
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
        except (BadArticleFormatError, IntegrityError):
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


def create_article_to_article_recs(
    model: ImplicitMF, model_id: int, external_ids: List[str], article_df: pd.DataFrame
):
    start_ts = time.time()
    created_recs = 0
    vector_similarities = get_similarities(model)
    vector_weights = get_weights(external_ids, article_df)
    vector_orders = get_orders(vector_similarities, vector_weights)

    for source_index, ranked_recommendation_indices in enumerate(vector_orders):
        source_external_id = external_ids[source_index]

        rec_ids = set()
        for recommendation_index in ranked_recommendation_indices[: MAX_RECS + 1]:
            if len(rec_ids) == MAX_RECS:
                break

            recommended_external_id = external_ids[recommendation_index]
            # If it's the article itself, skip it
            if recommended_external_id == source_external_id:
                continue

            matching_articles = article_df[
                article_df["external_id"] == recommended_external_id
            ]
            recommended_article_id = matching_articles["article_id"][0]
            # for distance, smaller values are more highly correlated
            # for score, higher values are more highly correlated
            score = vector_similarities[source_index][recommendation_index]
            # fix case when some scores are negative due to a rounding error
            score = max(score, 0.0)

            rec_id = create_rec(
                source_entity_id=int(source_external_id),
                model_id=model_id,
                recommended_article_id=recommended_article_id,
                score=score,
            )
            rec_ids.add(rec_id)
            created_recs += 1

    latency = time.time() - start_ts
    write_metric("rec_creation_time", latency, unit=Unit.SECONDS)
    write_metric("rec_creation_total", created_recs)


def get_similarities(model: ImplicitMF) -> np.array:
    vector_distances = distance.cdist(
        model.item_vectors, model.item_vectors, metric="cosine"
    )
    vector_similarities = 1 - vector_distances
    return vector_similarities


def get_weights(
    external_ids: List[str], article_df: pd.DataFrame, half_life: float = 10
) -> np.array:
    weights = np.ones(len(external_ids))
    publish_time_df = (
        article_df[["external_id", "published_at"]]
        .drop_duplicates("external_id")
        .set_index("external_id")
        .loc[external_ids]
    )
    publish_time_df["published_at"] = pd.to_datetime(publish_time_df.published_at)
    date_delta = (
        datetime.now(timezone.utc) - publish_time_df.published_at
    ).dt.total_seconds() / (3600 * 60 * 24)
    return preprocessors.apply_decay(weights, date_delta, half_life)


def get_orders(similarities: np.array, weights: np.array):
    similarities *= weights
    orders = similarities.argsort()[:, ::-1]
    return orders


def prepare_data(data_df: pd.DataFrame) -> pd.DataFrame:
    """
    :param data_df: DataFrame of activities collected from Google Analytics using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str)
    :param date_list:
    :param external_id_col:
    :param half_life:
    :return:
    """
    clean_df = preprocessors.fix_dtypes(data_df)
    sorted_df = preprocessors.time_activities(clean_df)
    filtered_df = preprocessors.filter_activities(sorted_df)
    return filtered_df


def format_data(
    prepared_df: pd.DataFrame,
    date_list: list = [],
    external_id_col: str = "external_id",
    half_life: float = 10.0,
) -> pd.DataFrame:
    """
    Format clickstream Google Analytics data into user-item matrix for training.

    :param prepared_df: DataFrame of activities collected from Google Analytics using job.py
        * Requisite fields: "session_date" (datetime.date), "client_id" (str), "external_id" (str),
            "event_action" (str), "event_category" (str), "duration" (timedelta)
    :param date_list: List of datetimes to forcefully include in all aggregates
    :param external_id_col: Name of column being used to denote articles
    :param half_life: Desired half life of time spent in days
    :return: DataFrame with one row for each user at each date of interest, and one column for each article
    """
    time_df = preprocessors.aggregate_time(
        prepared_df, date_list=date_list, external_id_col=external_id_col
    )
    exp_time_df = preprocessors.time_decay(time_df, half_life=half_life)

    return exp_time_df


def calculate_default_recs(prepared_df: pd.DataFrame) -> pd.Series:
    TOTAL_INTERACTIONS = 5000
    timed_interactions = prepared_df[~prepared_df.duration.isna()]
    recent_interactions = timed_interactions.nlargest(
        n=TOTAL_INTERACTIONS, columns=["activity_time"]
    )
    times = (
        recent_interactions[["external_id", "duration"]]
        .groupby("external_id")
        .sum()
        .sort_index()
    )
    pageviews = (
        recent_interactions[["external_id", "client_id"]]
        .groupby("external_id")
        .nunique("client_id")
        .sort_index()
    )
    times_per_view = times.duration / pageviews.client_id
    top_times_per_view = times_per_view.sort_values(ascending=False)
    return top_times_per_view


def create_default_recs(prepared_df: pd.DataFrame, article_df: pd.DataFrame) -> None:
    top_times_per_view = calculate_default_recs(prepared_df)
    weights = get_weights(top_times_per_view.index, article_df)
    # the most read article will have a perfect score of 1.0, all others will be a fraction of that
    scores = weights * top_times_per_view / max(top_times_per_view)
    top_scores = scores.nlargest(MAX_RECS)
    model_id = create_model(type=Type.POPULARITY.value)
    logging.info("Saving default recs to db...")
    for external_id, score in zip(top_times_per_view.index, top_scores):
        matching_articles = article_df[article_df["external_id"] == external_id]
        article_id = matching_articles["article_id"][0]
        rec_id = create_rec(
            source_entity_id="default",
            model_id=model_id,
            recommended_article_id=article_id,
            score=score,
        )

    set_current_model(model_id, Type.POPULARITY.value)
