import pytest
from requests.models import Response

from sites.validate import (
    validate_response,
    validate_status_code,
)


def _create_response(status_code: int) -> Response:
    response = Response()
    response.status_code = status_code
    return response


def _validate_good(response: Response) -> None:
    return None


def _validate_bad(response: Response) -> None:
    return "Some error message"


@pytest.fixture(scope="module")
def response_good() -> Response:
    return _create_response(200)


@pytest.fixture(scope="module")
def response_bad() -> Response:
    return _create_response(404)


def test_validate_status_code__good(response_good: Response) -> None:
    msg = validate_status_code(response_good)
    assert msg is None


def test_validate_stauts_code__bad(response_bad: Response) -> None:
    msg = validate_status_code(response_bad)
    assert type(msg) is str


def test_validate_response__single_good() -> None:
    msg = validate_response(Response(), [_validate_good])
    assert msg is None


def test_validate_response__single_bad() -> None:
    msg = validate_response(Response(), [_validate_bad])
    assert type(msg) is str


def test_validate_response__multiple_good() -> None:
    msg = validate_response(Response(), [_validate_good, _validate_good])
    assert msg is None


def test_validate_response__multiple_bad() -> None:
    msg = validate_response(Response(), [_validate_good, _validate_bad])
    assert type(msg) is str
