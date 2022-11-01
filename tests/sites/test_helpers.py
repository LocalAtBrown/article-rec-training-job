from requests.models import Response

from sites.helpers import validate_response


def _validate_good(response: Response) -> None:
    return None


def _validate_bad(response: Response) -> None:
    return "Some error message"


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
