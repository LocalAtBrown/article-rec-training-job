from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, datetime
from typing import Protocol, runtime_checkable

import pandera as pa
from article_rec_db.models import Page
from loguru import logger
from pydantic import ConfigDict, HttpUrl, validate_call

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


@runtime_checkable
class PageFetcher(Protocol):
    def fetch(self, urls: set[HttpUrl]) -> list[Page]:
        ...

    def post_fetch(self) -> None:
        ...


# ----- TASKS BASE CLASSES -----
class Task(ABC):
    @abstractmethod
    def execute(self) -> None:
        ...


class FetchesEvents:
    @staticmethod
    @pa.check_types
    def fetch_events(fetcher: EventFetcher) -> FetchedEventsDataFrame:
        df = fetcher.fetch()
        fetcher.post_fetch()
        return df


class FetchesPages:
    @staticmethod
    @validate_call(config=ConfigDict(arbitrary_types_allowed=True), validate_return=True)
    def fetch_pages(fetcher: PageFetcher, urls: set[HttpUrl]) -> list[Page]:
        pages = fetcher.fetch(urls)
        fetcher.post_fetch()
        return pages


# ----- TASKS -----
@dataclass
class UpdatePages(Task, FetchesEvents, FetchesPages):
    execution_timestamp: datetime
    event_fetcher: EventFetcher
    page_fetcher: PageFetcher
    # sa_session_factory: Type[Session]

    def execute(self) -> None:
        # First, fetch events
        logger.info(
            f"Fetching events from {self.event_fetcher.date_start} to {self.event_fetcher.date_end}",
            "to find pages to update",
        )
        df = self.fetch_events(self.event_fetcher)
        logger.info(f"Fetched {len(df)} events")

        # Then, fetch pages.
        # The fetcher is responsible for identifying which pages to fetch anew/create and which to update,
        # as well as including an Article object in a Page object that corresponds to the page's article.
        page_urls = set(df[FetchEventsSchema.page_url])
        logger.info(f"Found {len(page_urls)} URLs from events")
        pages = self.fetch_pages(self.page_fetcher, {HttpUrl(url) for url in page_urls})
        articles = [page.article[0] for page in pages if page.article is not None]
        logger.info(f"Fetched {len(pages)} pages and {len(articles)} articles")

        # Finally, upsert pages and articles in DB
        # TODO


@dataclass
class CreateRecommendations(Task, FetchesEvents):
    execution_timestamp: datetime
    event_fetcher: EventFetcher

    def execute(self) -> None:
        raise NotImplementedError
