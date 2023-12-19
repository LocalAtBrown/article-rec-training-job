from dataclasses import dataclass
from datetime import datetime

from pydantic import HttpUrl

from article_rec_training_job.shared.types.event_fetchers import (
    OutputSchema as FetchEventsSchema,
)
from article_rec_training_job.tasks.base import (
    FetchesEvents,
    FetchesPages,
    Task,
    WritesPages,
)
from article_rec_training_job.tasks.component_protocols import (
    EventFetcher,
    PageFetcher,
    PageWriter,
)


# ----- TASKS -----
@dataclass
class UpdatePages(Task, FetchesEvents, FetchesPages, WritesPages):
    execution_timestamp: datetime
    event_fetcher: EventFetcher
    page_fetcher: PageFetcher
    page_writer: PageWriter

    def execute(self) -> None:
        # First, fetch events
        df, metrics_fetch_events = self.fetch_events(self.event_fetcher)
        # TODO: Log or emit metrics

        # Then, fetch pages.
        # The fetcher is responsible for identifying which pages to fetch anew/create and which to update,
        # as well as including an Article object in a Page object that corresponds to the page's article.
        page_urls = set(df[FetchEventsSchema.page_url])
        pages, metrics_fetch_pages = self.fetch_pages(self.page_fetcher, urls={HttpUrl(url) for url in page_urls})
        # TODO: Log or emit metrics

        # Finally, upsert pages and articles in DB
        metrics_write_pages = self.write_pages(self.page_writer, pages=pages)  # noqa: F841
        # TODO: Log or emit metrics


@dataclass
class CreateRecommendations(Task, FetchesEvents):
    execution_timestamp: datetime
    event_fetcher: EventFetcher

    def execute(self) -> None:
        raise NotImplementedError
