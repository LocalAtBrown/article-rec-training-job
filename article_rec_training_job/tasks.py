from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol

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
    pass


# ----- TASKS -----
class Task(ABC):
    # @property
    # @abstractmethod
    # def execution_timestamp(self) -> datetime:
    #     ...

    @abstractmethod
    def execute(self) -> None:
        ...


@dataclass
class UpdatePages(Task):
    execution_timestamp: datetime
    event_fetcher: EventFetcher
    # page_fetcher: PageFetcher

    def execute(self) -> None:
        logger.info(
            f"Fetching events from {self.event_fetcher.date_start} to {self.event_fetcher.date_end}",
            "to find pages to update",
        )
        df = self.event_fetcher.fetch()
        self.event_fetcher.post_fetch()
        page_urls = set(df[FetchEventsSchema.page_url])
        logger.info(f"Fetched {len(df)} events and found {len(page_urls)} unique page URLs")


@dataclass
class CreateRecommendations(Task):
    execution_timestamp: datetime
    event_fetcher: EventFetcher

    def execute(self) -> None:
        raise NotImplementedError
