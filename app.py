import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum

from loguru import logger

from article_rec_training_job._0_fetch_events import EventFetcher, fetch_events
from article_rec_training_job.components._0_fetch_events import GA4EventFetcher


class Stage(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


class EventFetcherType(StrEnum):
    GA4 = "ga4"


@dataclass(frozen=True)
class Site:
    name: str
    ga4_property_id: str
    event_fetcher_type: EventFetcherType


def get_event_fetcher(site: Site, date_start: date, date_end: date) -> EventFetcher:
    match site.event_fetcher_type:
        case EventFetcherType.GA4:
            return GA4EventFetcher(
                gcp_project_id=os.environ["GCP_PROJECT_ID"],
                site_ga4_property_id=site.ga4_property_id,
                date_start=date_start,
                date_end=date_end,
            )
        case _:
            raise NotImplementedError(f"Event fetcher type {site.event_fetcher_type} not implemented")


def execute_job(stage: Stage) -> None:
    # If stage is local, load configuration as environment variables via .env
    if stage == Stage.LOCAL:
        from dotenv import load_dotenv

        load_dotenv()

    # Instantiate site
    site = Site(
        name=os.environ["SITE_NAME"],
        ga4_property_id=os.environ["SITE_GA4_PROPERTY_ID"],
        event_fetcher_type=EventFetcherType(os.environ["SITE_EVENT_FETCHER_TYPE"]),
    )

    # Set timestamp of job execution
    execution_timestamp = (
        datetime.strptime(os.environ["JOB_EXECUTION_TIMESTAMP_UTC"], "%Y-%m-%d %H:%M:%S.%f")
        if os.getenv("JOB_EXECUTION_TIMESTAMP_UTC") is not None
        else datetime.utcnow()
    )

    # ----- 1. FETCH EVENTS -----
    days_to_fetch_events = int(os.environ["NUM_DAYS_TO_FETCH_EVENTS"])
    date_end_fetch_events = execution_timestamp.date()
    date_start_fetch_events = date_end_fetch_events - timedelta(days=days_to_fetch_events - 1)

    event_fetcher = get_event_fetcher(site, date_start_fetch_events, date_end_fetch_events)

    logger.info(f"Fetching events for {site.name} from {date_start_fetch_events} to {date_end_fetch_events}...")
    df_events = fetch_events(event_fetcher)
    logger.info(f"Fetched events DataFrame shape: {df_events.shape}")

    # TODO: Next steps

    return df_events


if __name__ == "__main__":
    execute_job(stage=Stage.LOCAL)
