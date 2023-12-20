import pytest
from google.api_core.client_options import ClientOptions
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery


@pytest.fixture(scope="session")
def port_bigquery() -> int:
    # This needs to match the port specified in compose.yaml
    return 9050


@pytest.fixture(scope="session")
def project_id_bigquery() -> str:
    # This needs to match the project ID specified in compose.yaml
    return "test"


@pytest.fixture(scope="session")
def client_bigquery(port_bigquery, project_id_bigquery) -> bigquery.Client:
    return bigquery.Client(
        project_id_bigquery,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://0.0.0.0:{port_bigquery}"),
    )
