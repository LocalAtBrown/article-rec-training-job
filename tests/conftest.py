import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


@pytest.fixture(scope="session")
def port_bigquery() -> int:
    return 9050


@pytest.fixture(scope="session")
def client_bigquery(port_bigquery) -> bigquery.Client:
    return bigquery.Client(
        "test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://0.0.0.0:{port_bigquery}"),
    )
