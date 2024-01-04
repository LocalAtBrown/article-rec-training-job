import os
from pathlib import Path

import pytest
import yaml
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from app import load_config_from_file
from article_rec_training_job.config import Config


@pytest.fixture(scope="package")
def config_file_path() -> Path:
    return Path(os.path.dirname(__file__)) / "config-example.yaml"


@pytest.fixture(scope="package")
def config_env_value(config_file_path) -> str:
    with config_file_path.open("r") as f:
        config_dict = yaml.safe_load(f)

    return yaml.safe_dump(config_dict)


@pytest.fixture(scope="package")
def fake_postgres_db_url() -> str:
    return "postgresql://fake:5432/db"


@pytest.fixture(scope="function")
def set_config_env(config_env_value, fake_postgres_db_url) -> None:
    """
    Set appropriate environment variables for config, then restore original environment
    once the test is done.
    """
    original_env = dict(os.environ)
    os.environ.update({"JOB_CONFIG": config_env_value, "POSTGRES_DB_URL": fake_postgres_db_url})

    yield

    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture(scope="package")
def config(config_file_path) -> Config:
    return load_config_from_file(config_file_path)


@pytest.fixture(scope="package")
def fake_engine(fake_postgres_db_url) -> Engine:
    return create_engine(fake_postgres_db_url)


@pytest.fixture(scope="package")
def fake_sessionmaker(fake_engine) -> sessionmaker[Session]:
    return sessionmaker(bind=fake_engine)
