import docker
import pytest


@pytest.fixture(scope="session")
def docker_client() -> docker.DockerClient:
    return docker.from_env()


@pytest.fixture(scope="session")
def start_then_stop_bigquery(docker_client) -> None:
    container = docker_client.containers.run(
        "ghcr.io/goccy/bigquery-emulator:0.4.4",
        name="bigquery",
        detach=True,
        remove=True,
        environment={"project": "test", "dataset": "analytics_123456789", "data-from-yaml": "./tests/data/bigquery.yaml"},
    )
    yield
    container.stop()
