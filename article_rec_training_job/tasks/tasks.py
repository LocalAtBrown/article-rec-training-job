from dataclasses import dataclass, field

from pydantic import HttpUrl

from article_rec_training_job.shared.types.event_fetchers import (
    Metrics as FetchEventsMetrics,
)
from article_rec_training_job.shared.types.event_fetchers import (
    OutputSchema as FetchEventsSchema,
)
from article_rec_training_job.shared.types.page_fetchers import (
    Metrics as FetchPagesMetrics,
)
from article_rec_training_job.shared.types.page_writers import (
    Metrics as WritePagesMetrics,
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
    batch_components: list[tuple[EventFetcher, PageFetcher, PageWriter]]
    batch_metrics: list[tuple[FetchEventsMetrics, FetchPagesMetrics, WritePagesMetrics]] = field(
        init=False, repr=False, default_factory=list
    )

    def execute_one_batch(
        self, batch: tuple[EventFetcher, PageFetcher, PageWriter]
    ) -> tuple[FetchEventsMetrics, FetchPagesMetrics, WritePagesMetrics]:
        event_fetcher, page_fetcher, page_writer = batch

        # First, fetch events
        df, metrics_fetch_events = self.fetch_events(event_fetcher)

        # Then, fetch pages.
        # The fetcher is responsible for identifying which pages to fetch anew/create and which to update,
        # as well as including an Article object in a Page object that corresponds to the page's article.
        page_urls = set(df[FetchEventsSchema.page_url])
        pages, metrics_fetch_pages = self.fetch_pages(page_fetcher, urls={HttpUrl(url) for url in page_urls})

        # Finally, upsert pages and articles in DB
        metrics_write_pages = self.write_pages(page_writer, pages=pages)

        return metrics_fetch_events, metrics_fetch_pages, metrics_write_pages

    def execute(self) -> None:
        for batch in self.batch_components:
            metrics_one_batch = self.execute_one_batch(batch)
            self.batch_metrics.append(metrics_one_batch)

        # TODO: log metrics
