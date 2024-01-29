from datetime import date
from typing import Protocol, runtime_checkable

from article_rec_db.models import Page, Recommender
from article_rec_db.models.article import Language
from pydantic import HttpUrl

from article_rec_training_job.shared.types.article_recommenders import (
    Metrics as RecommendArticlesMetrics,
)
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
from article_rec_training_job.shared.types.recommendation_writers import (
    Metrics as WriteRecommendationsMetrics,
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


class TrafficBasedArticleRecommender(Protocol):
    max_recommendations: int
    allowed_languages: set[Language]

    def recommend(self, df_events: FetchEventsDataFrame) -> tuple[Recommender, RecommendArticlesMetrics]:
        ...


class RecommendationWriter(Protocol):
    def write(self, recommender: Recommender) -> WriteRecommendationsMetrics:
        ...
