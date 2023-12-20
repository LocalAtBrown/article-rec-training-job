from datetime import datetime
from uuid import UUID

import pytest
from article_rec_db.models import Article, Page
from article_rec_db.models.article import Language
from pydantic import HttpUrl
from sqlalchemy import select

from article_rec_training_job.components.page_writers.postgres import BaseWriter


@pytest.fixture(scope="module")
def pages() -> list[Page]:
    return [
        Page(url=HttpUrl("https://test.com/")),
        Page(
            url=HttpUrl("https://test.com/article-1/"),
            article=Article(
                site="test",
                id_in_site="1234",
                title="Article 1",
                description="Description of article 1",
                content="<p>Content of article 1</p>",
                site_published_at=datetime(2021, 1, 1, 0, 0, 0),
                site_updated_at=None,
                language=Language.ENGLISH,
                is_in_house_content=False,
            ),
        ),
        Page(
            url=HttpUrl("https://test.com/article-2/"),
            article=Article(
                site="test",
                id_in_site="1235",
                title="Article 2",
                description="Description of article 2",
                content="<p>Content of article 2</p>",
                site_published_at=datetime(2021, 1, 2, 0, 0, 0),
                site_updated_at=datetime(2021, 1, 3, 0, 0, 0),
                language=Language.SPANISH,
                is_in_house_content=True,
            ),
        ),
        Page(
            url=HttpUrl("https://test.com/article-3/"),
        ),
    ]


def test_base_writer_write(refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, pages):
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write(pages)

    assert metrics.num_new_pages_written == 4
    assert metrics.num_pages_ignored == 0
    assert metrics.num_articles_upserted == 2
    assert metrics.num_articles_ignored == 0
    assert metrics.time_taken_to_write_pages > 0

    with sa_session_factory_postgres() as session:
        # Find all pages and check that they were written correctly
        query_result = session.execute(select(Page)).all()
        pages = sorted([page for page, in query_result], key=lambda page: page.url)
        assert len(pages) == 4

        assert isinstance(pages[0].id, UUID)
        assert pages[0].url == "https://test.com/"
        assert isinstance(pages[0].db_created_at, datetime)
        assert pages[0].article is None

        assert isinstance(pages[1].id, UUID)
        assert pages[1].url == "https://test.com/article-1/"
        assert pages[1].article.page_id == pages[1].id
        assert isinstance(pages[1].db_created_at, datetime)
        assert isinstance(pages[1].article.db_created_at, datetime)
        assert pages[1].article.db_updated_at is None
        assert pages[1].article.site == "test"
        assert pages[1].article.id_in_site == "1234"
        assert pages[1].article.title == "Article 1"
        assert pages[1].article.description == "Description of article 1"
        assert pages[1].article.content == "<p>Content of article 1</p>"
        assert pages[1].article.site_published_at == datetime(2021, 1, 1, 0, 0, 0)
        assert pages[1].article.site_updated_at is None
        assert pages[1].article.language == Language.ENGLISH
        assert pages[1].article.is_in_house_content is False
        assert pages[1].article.embeddings == []
        assert pages[1].article.recommendations_where_this_is_source == []
        assert pages[1].article.recommendations_where_this_is_target == []

        assert isinstance(pages[2].id, UUID)
        assert pages[2].url == "https://test.com/article-2/"
        assert pages[2].article.page_id == pages[2].id
        assert isinstance(pages[2].db_created_at, datetime)
        assert isinstance(pages[2].article.db_created_at, datetime)
        assert pages[2].article.db_updated_at is None
        assert pages[2].article.site == "test"
        assert pages[2].article.id_in_site == "1235"
        assert pages[2].article.title == "Article 2"
        assert pages[2].article.description == "Description of article 2"
        assert pages[2].article.content == "<p>Content of article 2</p>"
        assert pages[2].article.site_published_at == datetime(2021, 1, 2, 0, 0, 0)
        assert pages[2].article.site_updated_at == datetime(2021, 1, 3, 0, 0, 0)
        assert pages[2].article.language == Language.SPANISH
        assert pages[2].article.is_in_house_content is True
        assert pages[2].article.embeddings == []
        assert pages[2].article.recommendations_where_this_is_source == []
        assert pages[2].article.recommendations_where_this_is_target == []


def test_base_writer_update_article_no_changes(
    refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, pages
):
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    writer.write(pages)

    # Now, update article 1 with no changes
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write([pages[1]])

    assert metrics.num_new_pages_written == 0
    assert metrics.num_pages_ignored == 1
    assert metrics.num_articles_upserted == 0
    assert metrics.num_articles_ignored == 1
    assert metrics.time_taken_to_write_pages > 0

    with sa_session_factory_postgres() as session:
        # Find article 1
        article = (
            session.execute(select(Article).where(Article.id_in_site == pages[1].article.id_in_site))
            .unique()
            .scalar_one_or_none()
        )

        assert article.title == "Article 1"
        assert article.description == "Description of article 1"
        assert article.content == "<p>Content of article 1</p>"
        assert article.site_updated_at is None


def test_base_writer_update_article_no_db_updated_timestamp(
    refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, pages
):
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    writer.write(pages)

    # Now, update article 1
    site_updated_at = datetime(2021, 1, 3, 0, 0, 0)  # This used to be None, thus the upsert should be triggered
    page_to_update = Page(
        url=pages[1].url,
        article=Article(
            site=pages[1].article.site,
            id_in_site=pages[1].article.id_in_site,
            title="Article 1 Updated title",
            description="Article 1 Updated description",
            content="<p>Article 1 Updated content</p>",
            site_published_at=pages[1].article.site_published_at,
            site_updated_at=site_updated_at,
            language=pages[1].article.language,
            is_in_house_content=pages[1].article.is_in_house_content,
        ),
    )
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write([page_to_update])

    assert metrics.num_new_pages_written == 0
    assert metrics.num_pages_ignored == 1
    assert metrics.num_articles_upserted == 1
    assert metrics.num_articles_ignored == 0
    assert metrics.time_taken_to_write_pages > 0

    with sa_session_factory_postgres() as session:
        # Find article 1
        article = (
            session.execute(select(Article).where(Article.id_in_site == page_to_update.article.id_in_site))
            .unique()
            .scalar_one_or_none()
        )

        assert article.title == "Article 1 Updated title"
        assert article.description == "Article 1 Updated description"
        assert article.content == "<p>Article 1 Updated content</p>"
        assert article.site_updated_at == site_updated_at


def test_base_writer_update_article_smaller_db_updated_timestamp(
    refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, pages
):
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    writer.write(pages)

    # Now, update article 2
    site_updated_at = datetime(2021, 1, 4, 0, 0, 0)  # This used to be 2021-01-03, thus the upsert should be triggered
    page_to_update = Page(
        url=pages[2].url,
        article=Article(
            site=pages[2].article.site,
            id_in_site=pages[2].article.id_in_site,
            title="Article 2 Updated title",
            description="Article 2 Updated description",
            content="<p>Article 2 Updated content</p>",
            site_published_at=pages[2].article.site_published_at,
            site_updated_at=site_updated_at,
            language=pages[2].article.language,
            is_in_house_content=pages[2].article.is_in_house_content,
        ),
    )
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write([page_to_update])

    assert metrics.num_new_pages_written == 0
    assert metrics.num_pages_ignored == 1
    assert metrics.num_articles_upserted == 1
    assert metrics.num_articles_ignored == 0
    assert metrics.time_taken_to_write_pages > 0

    with sa_session_factory_postgres() as session:
        # Find article 2
        article = (
            session.execute(select(Article).where(Article.id_in_site == page_to_update.article.id_in_site))
            .unique()
            .scalar_one_or_none()
        )

        assert article.title == "Article 2 Updated title"
        assert article.description == "Article 2 Updated description"
        assert article.content == "<p>Article 2 Updated content</p>"
        assert article.site_updated_at == site_updated_at


def test_base_writer_update_page_add_article(
    refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, pages
):
    """
    Covers cases where our slug-detector fails to detect an article from a page,
    and later on we find out that the page actually has an article and need
    to update it with the article metadata.
    """
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    writer.write(pages)

    # Now, update page 3 by adding an article to it
    page_to_update = Page(
        url=pages[3].url,
        article=Article(
            site="test",
            id_in_site="1236",
            title="Article 3",
            description="Description of article 3",
            content="<p>Content of article 3</p>",
            site_published_at=datetime(2021, 1, 3, 0, 0, 0),
            site_updated_at=datetime(2021, 1, 4, 0, 0, 0),
            language=Language.ENGLISH,
            is_in_house_content=False,
        ),
    )
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write([page_to_update])

    assert metrics.num_new_pages_written == 0
    assert metrics.num_pages_ignored == 1
    assert metrics.num_articles_upserted == 1
    assert metrics.num_articles_ignored == 0
    assert metrics.time_taken_to_write_pages > 0

    with sa_session_factory_postgres() as session:
        # Find page 3
        page = session.execute(select(Page).where(Page.url == page_to_update.url)).scalar_one_or_none()

        assert page.article.page_id == page.id
        assert page.article.site == "test"
        assert page.article.id_in_site == "1236"
        assert page.article.title == "Article 3"
        assert page.article.description == "Description of article 3"
        assert page.article.content == "<p>Content of article 3</p>"
        assert page.article.site_published_at == datetime(2021, 1, 3, 0, 0, 0)
        assert page.article.site_updated_at == datetime(2021, 1, 4, 0, 0, 0)
        assert page.article.language == Language.ENGLISH
        assert page.article.is_in_house_content is False
        assert page.article.embeddings == []
        assert page.article.recommendations_where_this_is_source == []
        assert page.article.recommendations_where_this_is_target == []
