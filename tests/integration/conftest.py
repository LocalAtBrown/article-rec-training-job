from collections.abc import Generator

import pytest
from article_rec_db.models import SQLModel
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery
from psycopg2.extensions import AsIs, register_adapter
from pydantic import AnyUrl
from sqlalchemy import text
from sqlalchemy.engine import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker


# ----- BIGQUERY FIXTURES -----
@pytest.fixture(scope="package")
def port_bigquery() -> int:
    # This needs to match the port specified in compose.yaml
    return 9050


@pytest.fixture(scope="package")
def project_id_bigquery() -> str:
    # This needs to match the project ID specified in compose.yaml
    return "test"


@pytest.fixture(scope="package")
def client_bigquery(port_bigquery, project_id_bigquery) -> bigquery.Client:
    return bigquery.Client(
        project_id_bigquery,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://0.0.0.0:{port_bigquery}"),
    )


# ----- POSTGRES FIXTURES -----
@pytest.fixture(scope="package")
def port_postgres() -> int:
    # This needs to match the port specified in compose.yaml
    return 5432


@pytest.fixture(scope="package")
def engine_postgres(port_postgres) -> Engine:
    return create_engine(f"postgresql://postgres:postgres@localhost:{port_postgres}/postgres")


@pytest.fixture(scope="package")
def sa_session_factory_postgres(engine_postgres) -> sessionmaker[Session]:
    return sessionmaker(engine_postgres)


@pytest.fixture(scope="package")
def psycopg2_adapt_unknown_types() -> None:
    register_adapter(AnyUrl, lambda url: AsIs(f"'{url}'"))


@pytest.fixture(scope="package")
def initialize_postgres_db(sa_session_factory_postgres, engine_postgres) -> Generator[None, None, None]:
    with sa_session_factory_postgres() as session:
        session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        session.commit()

    SQLModel.metadata.create_all(engine_postgres)
    yield
    SQLModel.metadata.drop_all(engine_postgres)


# scope="function" ensures that the records are dropped after each test
@pytest.fixture(scope="function")
def refresh_tables(sa_session_factory_postgres, initialize_postgres_db) -> Generator[None, None, None]:
    yield
    with sa_session_factory_postgres() as session:
        session.execute(text("TRUNCATE TABLE page CASCADE"))
        session.execute(text("TRUNCATE TABLE article CASCADE"))
        session.execute(text("TRUNCATE TABLE embedding CASCADE"))
        session.execute(text("TRUNCATE TABLE recommendation CASCADE"))
        session.execute(text("TRUNCATE TABLE recommender CASCADE"))
        session.commit()
