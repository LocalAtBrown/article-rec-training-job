from abc import ABC, abstractmethod

import pandera as pa
from article_rec_db.models import Page, Recommender
from pydantic import ConfigDict, HttpUrl, validate_call

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
from article_rec_training_job.tasks.component_protocols import (
    EventFetcher,
    PageFetcher,
    PageWriter,
    TrafficBasedArticleRecommender,
)


class Task(ABC):
    """
    Base class for tasks.
    """

    @abstractmethod
    def execute(self) -> None:
        ...


# Mixins that add functionality to tasks, abiding to a 1-to-1 correspondence to components
class FetchesEvents:
    @staticmethod
    @pa.check_types
    def fetch_events(component: EventFetcher) -> tuple[FetchEventsDataFrame, FetchEventsMetrics]:
        return component.fetch()


class FetchesPages:
    @staticmethod
    @validate_call(config=ConfigDict(arbitrary_types_allowed=True), validate_return=True)
    def fetch_pages(component: PageFetcher, urls: set[HttpUrl]) -> tuple[list[Page], FetchPagesMetrics]:
        return component.fetch(urls)


class WritesPages:
    @staticmethod
    def write_pages(component: PageWriter, pages: list[Page]) -> WritePagesMetrics:
        return component.write(pages)


class CreatesTrafficBasedRecommendations:
    @staticmethod
    def recommend_articles(
        component: TrafficBasedArticleRecommender, df_events: FetchEventsDataFrame
    ) -> tuple[Recommender, RecommendArticlesMetrics]:
        return component.recommend(df_events)
