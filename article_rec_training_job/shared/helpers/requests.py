from aiohttp import ClientResponse, ClientSession
from loguru import logger
from pydantic import HttpUrl
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    stop_after_attempt,
    wait_random_exponential,
)


async def request(
    url: HttpUrl, maximum_attempts: int, maximum_backoff: float, log_attempts: bool = False
) -> ClientResponse:
    """
    Performs an HTTP GET request to the given URL with random-exponential
    sleep before retrying.
    """

    def log_attempt(state: RetryCallState) -> None:
        if log_attempts:
            logger.info(f"Attempt {state.attempt_number} of {maximum_attempts} at requesting {url}")

    async with ClientSession() as session:
        async for attempt in AsyncRetrying(
            reraise=True,
            wait=wait_random_exponential(max=maximum_backoff),
            stop=stop_after_attempt(maximum_attempts),
            before=log_attempt,
        ):
            with attempt:
                async with session.get(str(url), raise_for_status=True) as response:
                    # Need this to avoid the "ConnectionClosed" error
                    await response.read()

    return response
