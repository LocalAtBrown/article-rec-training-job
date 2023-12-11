from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol, Type, runtime_checkable

import pandera as pa
from article_rec_db.models import Article, Page
from loguru import logger
from pydantic import ConfigDict, HttpUrl, validate_call
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

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
    sa_session_factory: Type[Session]

    pages: list[Page] = field(init=False, repr=False)
    pages_article: list[Page] = field(init=False, repr=False)

    def write_pages(self) -> None:
        with self.sa_session_factory() as session:
            # First, write pages while ignoring duplicates
            logger.info("Writing pages to DB")
            statement_write_pages = (
                insert(Page)
                .values([page.model_dump() for page in self.pages])
                .on_conflict_do_nothing(index_elements=[Page.url])
            )
            result_write_pages = session.execute(statement_write_pages)
            session.commit()
            logger.info(
                f"Wrote {result_write_pages.rowcount} new pages to DB and ignored {len(self.pages) - result_write_pages.rowcount} duplicates"
            )

            # Then, fetch all page IDs of all articles to be written and return a dict of page URL -> page ID
            logger.info("Fetching page IDs of articles to be written")
            statement_fetch_page_ids = select(Page.id, Page.url).where(
                Page.url.in_([page.url for page in self.pages_article])
            )
            result_fetch_page_ids = session.execute(statement_fetch_page_ids).all()
            dict_page_url_to_id = {HttpUrl(row[1]): row[0] for row in result_fetch_page_ids}

            # Finally, write articles. If there's a duplicate, update it
            logger.info("Writing articles to DB")
            statement_write_articles = insert(Article).values(
                [{**page.article.model_dump(), "page_id": dict_page_url_to_id[page.url]} for page in self.pages_article]
            )
            statement_write_articles = statement_write_articles.on_conflict_do_update(
                index_elements=[Article.site, Article.id_in_site],
                set_={
                    "title": statement_write_articles.excluded.title,
                    "description": statement_write_articles.excluded.description,
                    "content": statement_write_articles.excluded.content,
                    "site_updated_at": statement_write_articles.excluded.site_updated_at,
                    "language": statement_write_articles.excluded.language,
                    "is_in_house_content": statement_write_articles.excluded.is_in_house_content,
                    "db_updated_at": datetime.utcnow(),
                },
                where=(Article.site_updated_at != statement_write_articles.excluded.site_updated_at),
            ).returning(Article.page_id)
            result_write_articles = session.scalars(statement_write_articles).unique().all()
            session.commit()
            logger.info(
                f"Wrote or updated {len(result_write_articles)} articles to DB, "
                + f"and ignored {len(self.pages_article) - len(result_write_articles)} duplicates with no changes"
            )

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
        self.pages = self.fetch_pages(self.page_fetcher, {HttpUrl(url) for url in page_urls})
        self.pages_article = [page for page in self.pages if page.article is not None]
        logger.info(f"Fetched {len(self.pages)} pages, of which fetched {len(self.pages_article)} articles")

        # Finally, upsert pages and articles in DB
        self.write_pages()


@dataclass
class CreateRecommendations(Task, FetchesEvents):
    execution_timestamp: datetime
    event_fetcher: EventFetcher

    def execute(self) -> None:
        raise NotImplementedError
