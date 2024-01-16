import os
from collections.abc import Callable
from datetime import date, datetime, timedelta
from pathlib import Path

import click
import yaml
from loguru import logger
from psycopg2.extensions import AsIs, register_adapter
from pydantic import AnyUrl
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

# Components
from article_rec_training_job.components import (
    GA4BaseEventFetcher,
    PopularityBaseArticleRecommender,
    PostgresBasePageWriter,
    WPBasePageFetcher,
)

# Config
from article_rec_training_job.config import (
    Config,
    EventFetcherType,
    PageFetcherType,
    PageWriterType,
)
from article_rec_training_job.config import Task as TaskConfig
from article_rec_training_job.config import TaskType
from article_rec_training_job.config import (
    TrafficBasedArticleRecommender as TrafficBasedArticleRecommenderConfig,
)
from article_rec_training_job.config import (
    TrafficBasedArticleRecommenderType,
    create_config_object,
)

# Helpers
from article_rec_training_job.shared.helpers.itertools import batched

# Tasks
from article_rec_training_job.tasks import (
    CreateTrafficBasedRecommendations,
    UpdatePages,
)
from article_rec_training_job.tasks.base import Task
from article_rec_training_job.tasks.component_protocols import (
    EventFetcher,
    PageFetcher,
    PageWriter,
    TrafficBasedArticleRecommender,
)


def load_config_from_env() -> Config:
    """
    Loads config via the `JOB_CONFIG` env var, which stores a YAML string.
    """
    config_str = os.environ["JOB_CONFIG"]
    config_dict = yaml.safe_load(config_str)

    return create_config_object(config_dict=config_dict)


def load_config_from_file(path: Path) -> Config:
    """
    Loads config from a specified YAML file.
    """
    with path.open("r") as f:
        config_dict = yaml.safe_load(f)

    return create_config_object(config_dict=config_dict)


def create_event_fetcher_factory_dict(config: Config) -> dict[EventFetcherType, Callable[[date, date], EventFetcher]]:
    def factory_ga4(date_start: date, date_end: date) -> GA4BaseEventFetcher:
        config_component = config.get_component(EventFetcherType.GA4_BASE)
        return GA4BaseEventFetcher(
            gcp_project_id=config_component.params["gcp_project_id"],
            site_ga4_property_id=config_component.params["site_ga4_property_id"],
            date_start=date_start,
            date_end=date_end,
        )

    return {
        EventFetcherType.GA4_BASE: factory_ga4,
    }


def create_page_fetcher_factory_dict(config: Config) -> dict[PageFetcherType, Callable[[], PageFetcher]]:
    def factory_wp() -> WPBasePageFetcher:
        config_component = config.get_component(PageFetcherType.WP_BASE)
        return WPBasePageFetcher(
            site_name=config.job_globals.site,
            slug_from_path_regex=config_component.params["slug_from_path_regex"],
            tag_id_republished_content=config_component.params.get("tag_id_republished_content"),
            request_maximum_attempts=config_component.params.get("request_maximum_attempts", 10),
            request_maximum_backoff=config_component.params.get("request_maximum_backoff", 60),
            url_prefix_str=config_component.params["url_prefix"],
            language_from_path_regex=config_component.params.get("language_from_path_regex", dict()),
        )

    return {
        PageFetcherType.WP_BASE: factory_wp,
    }


def create_page_writer_factory_dict(config: Config) -> dict[PageWriterType, Callable[[], PageWriter]]:
    def factory_postgres() -> PostgresBasePageWriter:
        config_component = config.get_component(PageWriterType.POSTGRES_BASE)
        db_url = os.environ[config_component.params["env_db_url"]]

        # Register adapter for Pydantic AnyUrl type so that psycopg2 recognizes it
        # before we create the session factory
        register_adapter(AnyUrl, lambda url: AsIs(f"'{url}'"))

        # Create session factory
        engine = create_engine(db_url)
        sa_session_factory = sessionmaker(bind=engine)

        return PostgresBasePageWriter(
            sa_session_factory=sa_session_factory,
            force_update_despite_latest=config_component.params.get("force_update_despite_latest", False),
        )

    return {
        PageWriterType.POSTGRES_BASE: factory_postgres,
    }


def create_traffic_based_article_recommender_factory_dict(
    config_component: TrafficBasedArticleRecommenderConfig,
) -> dict[TrafficBasedArticleRecommenderType, Callable[[], TrafficBasedArticleRecommender]]:
    def factory_popularity() -> PopularityBaseArticleRecommender:
        db_url = os.environ[config_component.params["env_db_url"]]

        # Create session factory
        engine = create_engine(db_url)
        sa_session_factory = sessionmaker(bind=engine)

        return PopularityBaseArticleRecommender(
            sa_session_factory=sa_session_factory, max_recommendations=config_component.params["max_recommendations"]
        )

    return {
        TrafficBasedArticleRecommenderType.POPULARITY: factory_popularity,
    }


def create_task_update_pages(
    config_task: TaskConfig,
    event_fetcher_factory_dict: dict[EventFetcherType, Callable[[date, date], EventFetcher]],
    page_fetcher_factory_dict: dict[PageFetcherType, Callable[[], PageFetcher]],
    page_writer_factory_dict: dict[PageWriterType, Callable[[], PageWriter]],
) -> UpdatePages:
    # Grab params from config
    # Fallback date_end to today if not specified in config
    date_end: date = config_task.params.get("date_end", datetime.utcnow().date())
    # These two are not optional
    days_to_fetch_events: int = config_task.params["days_to_fetch_events"]
    days_to_fetch_events_per_batch: int = config_task.params["days_to_fetch_events_per_batch"]

    # Create component factories according to config
    event_fetcher_factory = event_fetcher_factory_dict[EventFetcherType(config_task.components["event_fetcher"])]
    page_fetcher_factory = page_fetcher_factory_dict[PageFetcherType(config_task.components["page_fetcher"])]
    page_writer_factory = page_writer_factory_dict[PageWriterType(config_task.components["page_writer"])]

    # Divide dates to fetch into batches and create batch components
    # If date_end is 2021-10-07, days_to_fetch_events is 7,
    # then dates_to_fetch will be [2021-10-01, 2021-10-02, ..., 2021-10-07] (in ascending order)
    dates_to_fetch = [date_end - timedelta(days=days_to_fetch_events - 1 - i) for i in range(days_to_fetch_events)]
    batched_dates_to_fetch = batched(dates_to_fetch, days_to_fetch_events_per_batch)
    batch_components = [
        (
            event_fetcher_factory(batch_dates[0], batch_dates[-1]),
            page_fetcher_factory(),
            page_writer_factory(),
        )
        for batch_dates in batched_dates_to_fetch
    ]

    return UpdatePages(batch_components=batch_components)


def create_task_create_traffic_based_recommendations(
    config_task: TaskConfig,
    event_fetcher_factory_dict: dict[EventFetcherType, Callable[[date, date], EventFetcher]],
    traffic_based_article_recommender_factory_dict: dict[
        TrafficBasedArticleRecommenderType, Callable[[], TrafficBasedArticleRecommender]
    ],
) -> CreateTrafficBasedRecommendations:
    # Grab params from config
    # Fallback date_end to today if not specified in config
    date_end: date = config_task.params.get("date_end", datetime.utcnow().date())
    days_to_fetch_events: int = config_task.params["days_to_fetch_events"]

    # Create component factories according to config
    event_fetcher_factory = event_fetcher_factory_dict[EventFetcherType(config_task.components["event_fetcher"])]
    traffic_based_article_recommender_factory = traffic_based_article_recommender_factory_dict[
        TrafficBasedArticleRecommenderType(config_task.components["traffic_based_article_recommender"])
    ]

    return CreateTrafficBasedRecommendations(
        event_fetcher=event_fetcher_factory(date_end - timedelta(days=days_to_fetch_events - 1), date_end),
        recommender=traffic_based_article_recommender_factory(),
    )


@click.command()
@click.option(
    "-c",
    "--config-file",
    "config_file_path",
    type=click.Path(exists=True, path_type=Path),
    required=False,
    help="Path to config file. If not specified, config will be loaded from environment in YAML text format.",
)
def execute_job(config_file_path: Path | None) -> None:
    # Load job config
    config = load_config_from_file(path=config_file_path) if config_file_path is not None else load_config_from_env()

    # Register adapter for Pydantic AnyUrl type so that psycopg2 recognizes it
    # before we create the session factory
    register_adapter(AnyUrl, lambda url: AsIs(f"'{url}'"))

    # Component factories
    event_fetcher_factory_dict = create_event_fetcher_factory_dict(config)
    page_fetcher_factory_dict = create_page_fetcher_factory_dict(config)
    page_writer_factory_dict = create_page_writer_factory_dict(config)
    traffic_based_article_recommender_factory_dict = create_traffic_based_article_recommender_factory_dict(config)

    logger.info(f"Executing job for site: {config.job_globals.site}...")

    tasks: list[Task] = []

    for config_task in config.tasks:
        match config_task.type:
            # ----- 1. UPDATE PAGES -----
            case TaskType.UPDATE_PAGES:
                task = create_task_update_pages(
                    config_task, event_fetcher_factory_dict, page_fetcher_factory_dict, page_writer_factory_dict
                )
                tasks.append(task)
            # ----- 2. CREATE TRAFFIC-BASED RECOMMENDATIONS -----
            case TaskType.CREATE_TRAFFIC_BASED_RECOMMENDATIONS:
                task = create_task_create_traffic_based_recommendations(
                    config_task, event_fetcher_factory_dict, traffic_based_article_recommender_factory_dict
                )
                tasks.append(task)

    for task in tasks:
        # Wrap task execution in try/except block to ensure all tasks are executed
        try:
            logger.info(f"Executing task {task.__class__.__name__}...")
            task.execute()
            logger.info(f"Task {task.__class__.__name__} completed successfully")
        except Exception:
            logger.exception(f"Task {task.__class__.__name__} failed")


if __name__ == "__main__":
    execute_job()
