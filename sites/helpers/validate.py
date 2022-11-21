from requests.exceptions import HTTPError
import logging
from typing import List
from requests.models import Response
from typing import Callable, Optional

# Custom types
ResponseValidator = Callable[[Response], Optional[str]]


def validate_response(page: Response, validate_funcs: List[ResponseValidator]) -> Optional[str]:
    # Go through validation functions one by one, stop as soon as a message gets returned
    for func in validate_funcs:
        error_msg = func(page)
        if error_msg is not None:
            return error_msg
    return None


def validate_status_code(page: Response) -> Optional[str]:
    # Would be curious to see non-200 responses that still go through
    if page.status_code != 200:
        logging.info(f"Requested with resp. status {page.status_code}: {page.url}")
    try:
        # Raise HTTPError if error code is 400 or more
        page.raise_for_status()
        return None
    except HTTPError as e:
        return f'Request failed with error code {page.status_code} and message "{e}"'
