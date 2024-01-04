from datetime import date

from article_rec_db.models.article import Language

from app import (
    batched,
    create_event_fetcher_factory_dict,
    create_page_fetcher_factory_dict,
    create_page_writer_factory_dict,
    create_update_pages_task,
    load_config_from_env,
    load_config_from_file,
)
from article_rec_training_job.components import (
    GA4BaseEventFetcher,
    PostgresBasePageWriter,
    WPBasePageFetcher,
)
from article_rec_training_job.config import (
    Config,
    EventFetcherType,
    PageFetcherType,
    PageWriterType,
)


def test_batched():
    assert list(batched([1, 2, 3, 4, 5], 2)) == [(1, 2), (3, 4), (5,)]


def _test_loaded_config(config: Config) -> None:
    assert config.job_globals.site == "site-name"

    assert config.components.event_fetchers[0].type == "ga4_base"
    assert config.components.event_fetchers[0].params["gcp_project_id"] == "gcp-project-id"
    assert config.components.event_fetchers[0].params["site_ga4_property_id"] == "123456789"

    assert config.components.page_fetchers[0].type == "wp_base"
    assert config.components.page_fetchers[0].params["url_prefix"] == "https://example.com"
    assert (
        rf"{config.components.page_fetchers[0].params['slug_from_path_regex']}"
        == r"^/20\d{2}/(0[1-9]|1[012])/(?P<slug>[a-zA-Z\d\-%]+)/$"
    )
    assert rf"{config.components.page_fetchers[0].params['language_from_path_regex']['es']}" == r"^/es/.*/$"
    assert config.components.page_fetchers[0].params["tag_id_republished_content"] == 42
    assert config.components.page_fetchers[0].params["request_maximum_attempts"] == 10
    assert config.components.page_fetchers[0].params["request_maximum_backoff"] == 60

    assert config.components.page_writers[0].type == "postgres_base"
    assert config.components.page_writers[0].params["env_db_url"] == "POSTGRES_DB_URL"

    assert config.tasks[0].type == "update_pages"
    assert config.tasks[0].components["event_fetcher"] == "ga4_base"
    assert config.tasks[0].components["page_fetcher"] == "wp_base"
    assert config.tasks[0].components["page_writer"] == "postgres_base"
    assert config.tasks[0].params["date_end"] == date(2023, 12, 7)
    assert config.tasks[0].params["days_to_fetch"] == 7
    assert config.tasks[0].params["days_to_fetch_per_batch"] == 2


def test_load_config_from_env(set_config_env):
    config = load_config_from_env()
    _test_loaded_config(config)


def test_load_config_from_file(config_file_path):
    config = load_config_from_file(config_file_path)
    _test_loaded_config(config)


def test_create_event_fetcher_factory_dict(config):
    factory_dict = create_event_fetcher_factory_dict(config)

    date_start = date(2021, 12, 1)
    date_end = date(2021, 12, 7)

    # ga4_base
    factory_ga4 = factory_dict[EventFetcherType.GA4_BASE]
    component = factory_ga4(date_start, date_end)
    assert isinstance(component, GA4BaseEventFetcher)
    assert component.gcp_project_id == "gcp-project-id"
    assert component.site_ga4_property_id == "123456789"
    assert component.date_start == date_start
    assert component.date_end == date_end


def test_create_page_fetcher_factory_dict(config):
    factory_dict = create_page_fetcher_factory_dict(config)

    # wp_base
    factory_wp = factory_dict[PageFetcherType.WP_BASE]
    component = factory_wp()
    assert isinstance(component, WPBasePageFetcher)
    assert component.site_name == "site-name"
    assert component.slug_from_path_regex == r"^/20\d{2}/(0[1-9]|1[012])/(?P<slug>[a-zA-Z\d\-%]+)/$"
    assert component.language_from_path_regex[Language.SPANISH] == r"^/es/.*/$"
    assert component.tag_id_republished_content == 42
    assert component.request_maximum_attempts == 10
    assert component.request_maximum_backoff == 60
    assert component.url_prefix_str == "https://example.com"


def test_create_page_writer_factory_dict(set_config_env, config, fake_postgres_db_url):
    factory_dict = create_page_writer_factory_dict(config)

    # postgres_base
    factory_postgres = factory_dict[PageWriterType.POSTGRES_BASE]
    component = factory_postgres()
    assert isinstance(component, PostgresBasePageWriter)
    assert component.sa_session_factory.kw["bind"].url.render_as_string() == fake_postgres_db_url


def test_create_update_pages_task(set_config_env, config):
    event_fetcher_factory_dict = create_event_fetcher_factory_dict(config)
    page_fetcher_factory_dict = create_page_fetcher_factory_dict(config)
    page_writer_factory_dict = create_page_writer_factory_dict(config)

    task = create_update_pages_task(
        config=config,
        event_fetcher_factory_dict=event_fetcher_factory_dict,
        page_fetcher_factory_dict=page_fetcher_factory_dict,
        page_writer_factory_dict=page_writer_factory_dict,
    )

    # 7 days, 2 days per batch -> 4 batches
    assert len(task.batch_components) == 4

    for event_fetcher, page_fetcher, page_writer in task.batch_components:
        assert isinstance(event_fetcher, GA4BaseEventFetcher)
        assert isinstance(page_fetcher, WPBasePageFetcher)
        assert isinstance(page_writer, PostgresBasePageWriter)

    INDEX_EVENT_FETCHER = 0

    assert task.batch_components[0][INDEX_EVENT_FETCHER].date_start == date(2023, 12, 1)
    assert task.batch_components[0][INDEX_EVENT_FETCHER].date_end == date(2023, 12, 2)

    assert task.batch_components[1][INDEX_EVENT_FETCHER].date_start == date(2023, 12, 3)
    assert task.batch_components[1][INDEX_EVENT_FETCHER].date_end == date(2023, 12, 4)

    assert task.batch_components[2][INDEX_EVENT_FETCHER].date_start == date(2023, 12, 5)
    assert task.batch_components[2][INDEX_EVENT_FETCHER].date_end == date(2023, 12, 6)

    # Last batch has only one day
    assert task.batch_components[3][INDEX_EVENT_FETCHER].date_start == date(2023, 12, 7)
    assert task.batch_components[3][INDEX_EVENT_FETCHER].date_end == date(2023, 12, 7)
