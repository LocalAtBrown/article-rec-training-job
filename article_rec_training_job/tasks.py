from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

import pandera as pa
from loguru import logger

from article_rec_training_job.shared.types.event_fetchers import (
    OutputDataFrame as FetchedEventsDataFrame,
)
from article_rec_training_job.shared.types.event_fetchers import (
    OutputSchema as FetchEventsSchema,
)


# ----- COMPONENT PROTOCOLS -----
class EventFetcher(Protocol):
    date_start: date
    date_end: date

    def fetch(self) -> FetchedEventsDataFrame:
        ...

    def post_fetch(self) -> None:
        ...


class PageFetcher(Protocol):
    raise NotImplementedError


# ----- TASKS BASE CLASSES -----
class Task(ABC):
    @abstractmethod
    def execute(self) -> None:
        ...


class FetchesEvents:
    @staticmethod
    @pa.check_types
    def fetch_events(event_fetcher: EventFetcher) -> FetchedEventsDataFrame:
        df = event_fetcher.fetch()
        event_fetcher.post_fetch()
        return df


# ----- TASKS -----
@dataclass
class UpdatePages(Task, FetchesEvents):
    execution_timestamp: datetime
    event_fetcher: EventFetcher
    # page_fetcher: PageFetcher

    def execute(self) -> None:
        logger.info(
            f"Fetching events from {self.event_fetcher.date_start} to {self.event_fetcher.date_end}",
            "to find pages to update",
        )
        df = self.fetch_events(self.event_fetcher)
        page_urls = set(df[FetchEventsSchema.page_url])
        logger.info(f"Fetched {len(df)} events and found {len(page_urls)} unique page URLs")


@dataclass
class CreateRecommendations(Task, FetchesEvents):
    execution_timestamp: datetime
    event_fetcher: EventFetcher

    def execute(self) -> None:
        raise NotImplementedError
