from datetime import date
from typing import Protocol, runtime_checkable

from article_rec_db.models import Page
from pydantic import HttpUrl

from article_rec_training_job.shared.types.event_fetchers import (
    Metrics as FetchEventsMetrics,
)
from article_rec_training_job.shared.types.event_fetchers import (
    OutputDataFrame as FetchEventsDataFrame,
)
from article_rec_training_job.shared.types.page_fetchers import (
    Metrics as FetchPagesMetrics,
)
from article_rec_training_job.shared.types.page_writers import (
    Metrics as WritePagesMetrics,
)


class EventFetcher(Protocol):
    date_start: date
    date_end: date

    def fetch(self) -> tuple[FetchEventsDataFrame, FetchEventsMetrics]:
        ...


@runtime_checkable
class PageFetcher(Protocol):
    request_maximum_attempts: int
    request_maximum_backoff: float

    def fetch(self, urls: set[HttpUrl]) -> tuple[list[Page], FetchPagesMetrics]:
        ...


class PageWriter(Protocol):
    def write(self, pages: list[Page]) -> WritePagesMetrics:
        ...
