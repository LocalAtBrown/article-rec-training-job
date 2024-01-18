from datetime import datetime

import pytest
from article_rec_db.models import Article, Page
from article_rec_db.models.article import Language
from pydantic import HttpUrl

from article_rec_training_job.shared.types.event_fetchers import (
    OutputDataFrame as EventsDataFrame,
)
from article_rec_training_job.shared.types.event_fetchers import (
    OutputSchema as EventsSchema,
)


@pytest.fixture(scope="package")
def url_1() -> HttpUrl:
    return HttpUrl("https://test.com/article-1/")


@pytest.fixture(scope="package")
def url_2() -> HttpUrl:
    return HttpUrl("https://test.com/article-2/")


@pytest.fixture(scope="package")
def url_3() -> HttpUrl:
    return HttpUrl("https://test.com/article-3/")


@pytest.fixture(scope="package")
def url_4() -> HttpUrl:
    return HttpUrl("https://test.com/article-4/")


@pytest.fixture(scope="package")
def article_english_in_house(url_1) -> Article:
    return (
        Article(
            page=Page(url=url_1),
            site="test",
            id_in_site="1234",
            title="Article 1",
            description="Description of article 1",
            content="<p>Content of article 1</p>",
            site_published_at=datetime(2021, 1, 2, 0, 0, 0),
            site_updated_at=None,
            language=Language.ENGLISH,
            is_in_house_content=True,
        ),
    )


@pytest.fixture(scope="package")
def article_english_not_in_house(url_2) -> Article:
    return Article(
        page=Page(url=url_2),
        site="test",
        id_in_site="2345",
        title="Article 2",
        description="Description of article 2",
        content="<p>Content of article 2</p>",
        site_published_at=datetime(2021, 1, 1, 0, 0, 0),
        site_updated_at=None,
        language=Language.ENGLISH,
        is_in_house_content=False,
    )


@pytest.fixture(scope="package")
def article_not_english_in_house(url_3) -> Article:
    return Article(
        page=Page(url=url_3),
        site="test",
        id_in_site="3456",
        title="Article 3",
        description="Description of article 3",
        content="<p>Content of article 3</p>",
        site_published_at=datetime(2021, 1, 1, 0, 0, 0),
        site_updated_at=None,
        language=Language.SPANISH,
        is_in_house_content=True,
    )


@pytest.fixture(scope="package")
def article_not_english_not_in_house(url_4) -> Article:
    return Article(
        page=Page(url=url_4),
        site="test",
        id_in_site="4567",
        title="Article 4",
        description="Description of article 4",
        content="<p>Content of article 4</p>",
        site_published_at=datetime(2021, 1, 1, 0, 0, 0),
        site_updated_at=None,
        language=Language.SPANISH,
        is_in_house_content=False,
    )


@pytest.fixture(scope="package")
def user_id_1() -> str:
    return "123456789.1234567890"


@pytest.fixture(scope="package")
def user_id_2() -> str:
    return "234567890.2345678901"


@pytest.fixture(scope="package")
def events(url_1, url_2, url_3, url_4, user_id_1, user_id_2) -> EventsDataFrame:
    return EventsDataFrame(
        [
            # English, in-house article
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "page_view",
                EventsSchema.page_url: url_1,
                EventsSchema.engagement_time_msec: None,
                EventsSchema.user_id: user_id_1,
            },
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "scroll_custom",
                EventsSchema.page_url: url_1,
                EventsSchema.engagement_time_msec: 200,
                EventsSchema.user_id: user_id_1,
            },
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "user_engagement",
                EventsSchema.page_url: url_1,
                EventsSchema.engagement_time_msec: 5000,
                EventsSchema.user_id: user_id_1,
            },
            # English, 3rd-party article
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "user_engagement",
                EventsSchema.page_url: url_2,
                EventsSchema.engagement_time_msec: 1000,
                EventsSchema.user_id: user_id_1,
            },
            # Non-English, in-house article
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "user_engagement",
                EventsSchema.page_url: url_3,
                EventsSchema.engagement_time_msec: 6000,
                EventsSchema.user_id: user_id_2,
            },
            # Non-English, 3rd-party article
            {
                EventsSchema.event_timestamp: datetime.utcnow(),
                EventsSchema.event_name: "user_engagement",
                EventsSchema.page_url: url_4,
                EventsSchema.engagement_time_msec: 1000,
                EventsSchema.user_id: user_id_2,
            },
        ]
    )
