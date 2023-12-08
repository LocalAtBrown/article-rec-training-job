from datetime import datetime, timedelta
from enum import StrEnum

from loguru import logger

from article_rec_training_job.components import GA4BaseEventFetcher, WPBasePageFetcher
from article_rec_training_job.config import (
    Config,
    EventFetcherType,
    PageFetcherType,
    create_config_object,
)
from article_rec_training_job.tasks import Task, UpdatePages


class Stage(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


def load_config(stage: Stage) -> Config:
    if stage == Stage.LOCAL:
        import yaml

        with open("config.yaml", "r") as f:
            config_dict = yaml.safe_load(f)
    else:
        raise NotImplementedError(f"Stage {stage} not implemented")

    return create_config_object(config_dict)


def create_update_pages_task(config: Config) -> UpdatePages:
    task_config = config.tasks.update_pages

    if task_config is None:
        raise ValueError("Task update_pages is not configured")

    execution_timestamp = task_config.execution_timestamp_utc or datetime.utcnow()
    date_end = execution_timestamp.date()
    date_start = date_end - timedelta(days=task_config.event_fetcher.params["num_days_to_fetch"] - 1)

    match task_config.event_fetcher.type:
        case EventFetcherType.GA4_BASE:
            event_fetcher = GA4BaseEventFetcher(
                gcp_project_id=task_config.event_fetcher.params["gcp_project_id"],
                site_ga4_property_id=task_config.event_fetcher.params["site_ga4_property_id"],
                date_start=date_start,
                date_end=date_end,
            )

    match task_config.page_fetcher.type:
        case PageFetcherType.WP_BASE:
            page_fetcher = WPBasePageFetcher(
                site_name=config.site,
                url_prefix_str=task_config.page_fetcher.params["url_prefix"],
                slug_from_path_regex=task_config.page_fetcher.params["slug_from_path_regex"],
                language_from_path_regex=task_config.page_fetcher.params.get("language_from_path_regex", dict()),
                tag_id_republished_content=task_config.page_fetcher.params.get("tag_id_republished_content"),
                request_maximum_attempts=task_config.page_fetcher.params.get("request_maximum_attempts", 10),
                request_maximum_backoff=task_config.page_fetcher.params.get("request_maximum_backoff", 60),
            )

    return UpdatePages(
        execution_timestamp=execution_timestamp,
        event_fetcher=event_fetcher,
        page_fetcher=page_fetcher,
    )


def execute_job(stage: Stage) -> None:
    config = load_config(stage=stage)

    logger.info(f"Executing job for site: {config.site}...")

    tasks: list[Task] = []

    # ----- 1. UPDATE PAGES -----
    if config.tasks.update_pages is not None:
        tasks.append(create_update_pages_task(config))

    # ----- 2. CREATE RECOMMENDATIONS -----
    # TODO: Create recommendations task

    for task in tasks:
        # Wrap task execution in try/except block to ensure all tasks are executed
        try:
            logger.info(f"Executing task {task.__class__.__name__}...")
            task.execute()
            logger.info(f"Task {task.__class__.__name__} completed successfully")
        except Exception:
            logger.exception(f"Task {task.__class__.__name__} failed")


if __name__ == "__main__":
    execute_job(stage=Stage.LOCAL)
