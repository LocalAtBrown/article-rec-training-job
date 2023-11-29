import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Protocol

import pandas as pd
from loguru import logger

from article_rec_training_job._0_fetch_events import GA4EventFetcher as _GA4EventFetcher


class Stage(StrEnum):
    LOCAL = "local"
    DEV = "dev"
    PROD = "prod"


class SiteName(StrEnum):
    DALLAS_FREE_PRESS = "dallas-free-press"


class EventFetcherType(StrEnum):
    GA4 = "ga4"


@dataclass(frozen=True)
class Site:
    name: SiteName
    ga4_property_id: str
    event_fetcher_type: EventFetcherType


class EventFetcher(Protocol):
    site: Site
    date_start: date
    date_end: date

    def fetch(self) -> pd.DataFrame:
        ...

    def post_fetch(self) -> None:
        ...


class GA4EventFetcher(_GA4EventFetcher):
    def __init__(self, gcp_project_id: str, site: Site, date_start: date, date_end: date) -> None:
        super().__init__(
            gcp_project_id=gcp_project_id,
            site_ga4_property_id=site.ga4_property_id,
            date_start=date_start,
            date_end=date_end,
        )
        self.site = site

    def fetch(self) -> pd.DataFrame:
        return super().fetch()

    def post_fetch(self) -> None:
        num_tables_exist = sum([table.exists for table in self.tables])
        num_queries_executed = sum([query.executed for query in self.queries])
        num_queries_use_cache = sum([query.execution_uses_cache for query in self.queries])
        # total_bytes_processed = sum([query.total_bytes_processed for query in self.queries])
        # total_bytes_billed = sum([query.total_bytes_billed for query in self.queries])

        logger.info(f"{num_tables_exist} tables out of {len(self.tables)} exist")
        logger.info(f"{num_queries_executed} queries out of {len(self.queries)} executed")
        logger.info(f"{num_queries_use_cache} queries out of {len(self.queries)} used cache")


def fetch_events(fetcher: EventFetcher) -> pd.DataFrame:
    df = fetcher.fetch()
    fetcher.post_fetch()
    return df


def execute_job(stage: Stage) -> None:
    # Prepare
    if stage == Stage.LOCAL:
        from dotenv import load_dotenv

        load_dotenv()

    site = Site(
        name=os.environ["SITE_NAME"],
        ga4_property_id=os.environ["SITE_GA4_PROPERTY_ID"],
        event_fetcher_type=os.environ["SITE_EVENT_FETCHER_TYPE"],
    )

    timestamp_job_execution = datetime.utcnow()

    # 1. FETCH EVENTS
    days_to_fetch_events = int(os.environ["NUM_DAYS_TO_FETCH_EVENTS"])
    date_end_fetch_events = timestamp_job_execution.date()
    match site.event_fetcher_type:
        case EventFetcherType.GA4:
            fetcher = GA4EventFetcher(
                gcp_project_id=os.environ["GCP_PROJECT_ID"],
                site=site,
                date_start=date_end_fetch_events - timedelta(days=days_to_fetch_events - 1),
                date_end=date_end_fetch_events,
            )
        case _:
            raise NotImplementedError(f"Event fetcher type {site.event_fetcher_type} not implemented")
    df_events = fetch_events(fetcher)

    return df_events


if __name__ == "__main__":
    execute_job(stage=Stage.LOCAL)
