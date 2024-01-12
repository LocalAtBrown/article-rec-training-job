from dataclasses import dataclass, field
from datetime import datetime
from typing import final

from article_rec_db.models import Article, Page
from loguru import logger
from pydantic import HttpUrl
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from article_rec_training_job.shared.helpers.time import get_elapsed_time
from article_rec_training_job.shared.types.page_writers import Metrics


@dataclass
class BaseWriter:
    """
    Writer component that writes pages to a Postgres database
    whose tables are defined by the article-rec-db package.
    """

    # SQLA sessionmaker
    sa_session_factory: sessionmaker[Session]

    # Whether or not to force-update articles even if the site_updated_at
    # field is not changing. This is useful when we make changes to the database
    # schema (e.g., adding new columns to the article table) and want to force
    # an update during backfilling to populate data in all existing articles.
    force_update_despite_latest: bool

    num_new_pages_written: int = field(init=False, repr=False)
    num_pages_ignored: int = field(init=False, repr=False)
    num_articles_upserted: int = field(init=False, repr=False)
    num_articles_ignored: int = field(init=False, repr=False)
    time_taken_to_write_pages: float = field(init=False, repr=False)

    @get_elapsed_time
    def _write(self, pages: list[Page]) -> None:
        """
        Main writing logic. Writes pages & articles to the DB.
        """
        with self.sa_session_factory() as session:
            # First, write pages while ignoring duplicates
            logger.info("Writing pages to DB")
            statement_write_pages = (
                insert(Page)
                .values([page.model_dump() for page in pages])
                .on_conflict_do_nothing(index_elements=[Page.url])  # type: ignore
            )
            result_write_pages = session.execute(statement_write_pages)
            session.commit()
            # Save metrics
            self.num_new_pages_written = result_write_pages.rowcount
            self.num_pages_ignored = len(pages) - self.num_new_pages_written

            # Then, fetch all page IDs of all articles to be written and return a dict of page URL -> page ID
            logger.info("Fetching page IDs of articles to be written")
            pages_article = [page for page in pages if page.article is not None]
            statement_fetch_page_ids = select(
                Page.id, Page.url  # type: ignore # (see: https://github.com/tiangolo/sqlmodel/issues/271)
            ).where(
                Page.url.in_([page.url for page in pages_article])  # type: ignore
            )
            result_fetch_page_ids = session.execute(statement_fetch_page_ids).all()
            dict_page_url_to_id = {HttpUrl(row[1]): row[0] for row in result_fetch_page_ids}

            # Finally, write articles. If there's a duplicate, and that duplicate has updated information, update it
            logger.info("Writing articles to DB")
            statement_write_articles = insert(Article).values(
                [{**page.article.model_dump(), "page_id": dict_page_url_to_id[page.url]} for page in pages_article]  # type: ignore
            )
            # Obviously, we don't want to update an article if there's nothing new to update,
            # so we're relying on the site_updated_at field to determine whether or not to update
            condition_upsert_articles = (
                # If the site_updated_at field is changing from None to a timestamp, we update.
                (Article.site_updated_at == None)  # type: ignore  # noqa: E711
                & (statement_write_articles.excluded.site_updated_at != None)  # noqa: E711
            ) | (
                # If the site_updated_at field is changing from a timestamp to a newer timestamp, we also update.
                (Article.site_updated_at != None)  # type: ignore  # noqa: E711
                & (statement_write_articles.excluded.site_updated_at != None)  # noqa: E711
                & (Article.site_updated_at < statement_write_articles.excluded.site_updated_at)
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
                # Bypass the condition if we're forcing an update
                where=None if self.force_update_despite_latest else condition_upsert_articles,
            ).returning(Article.page_id)
            result_write_articles = session.scalars(statement_write_articles).unique().all()
            session.commit()
            # Save metrics
            self.num_articles_upserted = len(result_write_articles)
            self.num_articles_ignored = len(pages_article) - self.num_articles_upserted

    @final
    def write(self, pages: list[Page]) -> Metrics:
        self.time_taken_to_write_pages, _ = self._write(pages)
        return Metrics(
            num_new_pages_written=self.num_new_pages_written,
            num_pages_ignored=self.num_pages_ignored,
            num_articles_upserted=self.num_articles_upserted,
            num_articles_ignored=self.num_articles_ignored,
            time_taken_to_write_pages=self.time_taken_to_write_pages,
        )
