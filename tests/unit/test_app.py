from datetime import date

from app import (  # create_event_fetcher_factory_dict,; create_page_fetcher_factory_dict,; create_page_writer_factory_dict,; create_update_pages_task,
    batched,
    create_sa_session_factory,
    load_config_from_env,
    load_config_from_file,
)
from article_rec_training_job.config import Config


def test_batched():
    assert list(batched([1, 2, 3, 4, 5], 2)) == [(1, 2), (3, 4), (5,)]


def _test_loaded_config(config: Config) -> None:
    assert config.job_globals.site == "site-name"
    assert config.job_globals.env_postgres_db_url == "POSTGRES_DB_URL"

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
    assert config.components.page_writers[0].params == dict()

    assert config.tasks[0].type == "update_pages"
    assert config.tasks[0].components["event_fetcher"] == "ga4_base"
    assert config.tasks[0].components["page_fetcher"] == "wp_base"
    assert config.tasks[0].components["page_writer"] == "postgres_base"
    assert config.tasks[0].params["date_end"] == date(2023, 12, 7)
    assert config.tasks[0].params["days_to_fetch"] == 7
    assert config.tasks[0].params["days_to_fetch_per_batch"] == 1


def test_load_config_from_env(set_config_env):
    config = load_config_from_env()
    _test_loaded_config(config)


def test_load_config_from_file(config_file_path):
    config = load_config_from_file(config_file_path)
    _test_loaded_config(config)


def test_create_sa_session_factory(set_config_env, config, fake_postgres_db_url):
    sa_session_factory = create_sa_session_factory(config)
    assert sa_session_factory.kw["bind"].url.render_as_string() == fake_postgres_db_url
