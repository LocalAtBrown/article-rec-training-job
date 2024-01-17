from dataclasses import dataclass
from typing import final

from article_rec_db.models import Article, Page, Recommendation, Recommender
from article_rec_db.models.recommender import RecommendationType
from pydantic import HttpUrl
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from article_rec_training_job.shared.helpers.time import get_elapsed_time
from article_rec_training_job.shared.helpers.urllib import clean_url
from article_rec_training_job.shared.types.article_recommenders import Metrics, Strategy
from article_rec_training_job.shared.types.event_fetchers import (
    OutputDataFrame as EventsDataFrame,
)
from article_rec_training_job.shared.types.event_fetchers import (
    OutputSchema as EventsSchema,
)


@dataclass
class BaseRecommender:
    """
    Base popularity article recommender that recommends articles with the
    most engagement time.
    """

    # Maximum number of recommendations to return
    max_recommendations: int

    # SQLAlchemy session factory
    sa_session_factory: sessionmaker[Session]

    def get_articles_from_urls(self, urls: list[HttpUrl]) -> dict[HttpUrl, Article]:
        """
        Given a list of page URLs, returns a subset of those URLs
        """
        with self.sa_session_factory() as session:
            statement = select(Article, Page).where(
                # JOIN condition
                (Article.page_id == Page.id)
                # WHERE condition: page URL is in the list of URLs
                & (Page.url.in_(urls))
                # WHERE condition: article is in-house content
                & (Article.is_in_house_content == True)  # noqa: E712
            )
            results = session.execute(statement).unique()

            return {HttpUrl(article.page.url): article for article, _ in results}

    @get_elapsed_time
    def _recommend(self, df_events: EventsDataFrame) -> Recommender:
        # Clean URLs (remove query params and fragments)
        df_events[EventsSchema.page_url] = df_events[EventsSchema.page_url].apply(HttpUrl).apply(clean_url)

        # Group by page URL and sum engagement time
        series_engagement_time = df_events.groupby(EventsSchema.page_url)[EventsSchema.engagement_time_msec].sum()

        # Filter out entries that are not articles
        page_urls = series_engagement_time.index.tolist()
        dict_articles = self.get_articles_from_urls(page_urls)
        series_engagement_time = series_engagement_time[dict_articles.keys()]

        # Sort by engagement time, take top N entries, and convert from milliseconds to seconds
        series_engagement_time = series_engagement_time.sort_values(ascending=False).head(self.max_recommendations) / 1000

        # Create recommendations
        recommendations = [
            Recommendation(
                source_article=None,
                target_article=dict_articles[url],
                score=engagement_time_seconds,
            )
            for url, engagement_time_seconds in series_engagement_time.items()
        ]
        recommender = Recommender(
            strategy=Strategy.POPULARITY,
            recommendation_type=RecommendationType.DEFAULT_AKA_NO_SOURCE,
            recommendations=recommendations,
        )

        return recommender

    @final
    def recommend(self, df_events: EventsDataFrame) -> tuple[Recommender, Metrics]:
        """
        Main function to create recommendations. The returned Recommender
        includes recommendations as well as (if any) embeddings.
        """
        time_taken_to_create_recommendations, recommender = self._recommend(df_events)
        metrics = Metrics(
            time_taken_to_create_recommendations=time_taken_to_create_recommendations,
            num_recommendations_created=len(recommender.recommendations),
        )
        return recommender, metrics
