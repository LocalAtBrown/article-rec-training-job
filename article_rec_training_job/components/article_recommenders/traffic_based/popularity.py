from dataclasses import dataclass
from typing import final

from article_rec_db.models import Article, Page, Recommender
from pydantic import HttpUrl
from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from article_rec_training_job.shared.helpers.urllib import clean_url
from article_rec_training_job.shared.types.article_recommenders import Metrics
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

    @final
    def recommend(self, df_events: EventsDataFrame) -> tuple[Recommender, Metrics]:
        """
        Main function to create recommendations. The returned Recommender
        includes recommendations as well as (if any) embeddings.
        """
        # Clean URLs (remove query params and fragments)
        df_events[EventsSchema.page_url] = df_events[EventsSchema.page_url].apply(HttpUrl).apply(clean_url)

        # Group by page URL and sum engagement time
        series_engagement_time = df_events.groupby(EventsSchema.page_url)[EventsSchema.engagement_time_msec].sum()

        # Filter out entries that are not articles
        page_urls = series_engagement_time.index.tolist()
        dict_articles = self.get_articles_from_urls(page_urls)
        series_engagement_time = series_engagement_time[dict_articles.keys()]

        # Sort by engagement time and take top N entries
        series_engagement_time = series_engagement_time.sort_values(ascending=False).head(self.max_recommendations)
