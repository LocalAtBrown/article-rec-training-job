from datetime import datetime, timezone
from uuid import UUID

import numpy as np
import pytest
from article_rec_db.models import Article, Embedding, Page, Recommendation, Recommender
from article_rec_db.models.embedding import MAX_EMBEDDING_DIMENSIONS
from article_rec_db.models.recommender import RecommendationType
from sqlalchemy import select

from article_rec_training_job.components.recommendation_writers.postgres import (
    BaseWriter,
)


@pytest.fixture(scope="function")
def article_1() -> Article:
    page = Page(
        id=UUID(int=1),
        url="https://example.com/example-article-1/",
    )
    return Article(
        site="example-site",
        id_in_site="1234",
        title="Example Article 1",
        content="<p>Content</p>",
        site_published_at=datetime.now(timezone.utc),
        page=page,
    )


@pytest.fixture(scope="function")
def article_2() -> Article:
    page = Page(
        id=UUID(int=2),
        url="https://example.com/example-article-2/",
    )
    return Article(
        site="example-site",
        id_in_site="2345",
        title="Example Article 2",
        content="<p>Content</p>",
        site_published_at=datetime.now(timezone.utc),
        page=page,
    )


@pytest.fixture(scope="function")
def recommender(article_1, article_2) -> Recommender:
    embedding_1 = Embedding(article=article_1, vector=[0.1] * MAX_EMBEDDING_DIMENSIONS)
    embedding_2 = Embedding(article=article_2, vector=[0.4] * MAX_EMBEDDING_DIMENSIONS)
    recommendation = Recommendation(source_article=article_1, target_article=article_2, score=0.9)

    return Recommender(
        strategy="fake-strategy",
        recommendation_type=RecommendationType.SOURCE_TARGET_INTERCHANGEABLE,
        recommendations=[recommendation],
        embeddings=[embedding_1, embedding_2],
    )


@pytest.fixture(scope="function")
def write_articles_to_postgres(
    refresh_tables, sa_session_factory_postgres, psycopg2_adapt_unknown_types, article_1, article_2
) -> None:
    with sa_session_factory_postgres() as session:
        session.add_all([article_1, article_2])
        session.commit()


def test_base_writer(recommender, write_articles_to_postgres, sa_session_factory_postgres, article_1, article_2):
    writer = BaseWriter(sa_session_factory=sa_session_factory_postgres)
    metrics = writer.write(recommender)

    assert metrics.time_taken_to_write_recommendations > 0
    assert metrics.num_recommendations_written == 1
    assert metrics.num_embeddings_written == 2

    with sa_session_factory_postgres() as session:
        recommenders = session.execute(select(Recommender)).scalars().all()
        assert len(recommenders) == 1

        recommender_written = recommenders[0]
        assert recommender_written.strategy == recommender.strategy == "fake-strategy"
        assert (
            recommender_written.recommendation_type
            == recommender.recommendation_type
            == RecommendationType.SOURCE_TARGET_INTERCHANGEABLE
        )
        assert len(recommender_written.recommendations) == 1
        assert len(recommender_written.embeddings) == 2

        assert recommender_written.recommendations[0].source_article.page_id == article_1.page_id
        assert recommender_written.recommendations[0].target_article.page_id == article_2.page_id

        assert recommender_written.embeddings[0].article.page_id == article_1.page_id
        assert recommender_written.embeddings[1].article.page_id == article_2.page_id

        assert np.isclose(recommender_written.embeddings[0].vector, [0.1] * MAX_EMBEDDING_DIMENSIONS).all()
        assert np.isclose(recommender_written.embeddings[1].vector, [0.4] * MAX_EMBEDDING_DIMENSIONS).all()
