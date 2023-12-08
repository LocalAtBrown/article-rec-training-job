from enum import Enum
from urllib.parse import urlunparse

import nh3
from aiohttp import ClientResponse, ClientSession
from loguru import logger
from pydantic import HttpUrl
from tenacity import AsyncRetrying, stop_after_attempt, wait_random_exponential


class HTMLAllowedEntities(Enum):
    TAGS = {"a", "img", "video", "h1", "h2", "h3", "strong", "em", "p", "ul", "ol", "li", "br", "sub", "sup", "hr"}
    ATTRIBUTES = {
        "a": {"href", "name", "target", "title", "id"},
        "img": {"src", "alt", "title"},
        "video": {"src", "alt", "title"},
    }


def build_url(scheme: str, host: str, path: str = "/", query: str = "", fragment: str = "") -> HttpUrl:
    """
    Builds a URL from its components.
    """
    # Uses urllib.parse.urlunparse() instead of HttpUrl.build() because the latter
    # tends to create double slashes in the path, and the logic to clean that is uncessarily complicated
    return HttpUrl(urlunparse((scheme, host, path, "", query, fragment)))


def clean_url(url: HttpUrl) -> HttpUrl:
    """
    Cleans URL by removing all query parameters and fragments.
    """
    url_new = build_url(scheme=url.scheme, host=url.host, path=url.path)
    return url_new


def clean_html(html: str) -> str:
    """
    Cleans HTML by removing all tags and attributes that are not explicitly allowed.
    """
    return nh3.clean(html, tags=HTMLAllowedEntities.TAGS.value, attributes=HTMLAllowedEntities.ATTRIBUTES.value)


async def request(url: HttpUrl, maximum_attempts: int, maximum_backoff: int) -> ClientResponse:
    """
    Performs an HTTP GET request to the given URL with random-exponential
    sleep before retrying.
    """
    async with ClientSession() as session:
        async for attempt in AsyncRetrying(
            reraise=True,
            wait=wait_random_exponential(max=maximum_backoff),
            stop=stop_after_attempt(maximum_attempts),
            before=lambda state: logger.info(f"Attempt {state.attempt_number} of {maximum_attempts} at requesting {url}"),
        ):
            with attempt:
                async with session.get(str(url), raise_for_status=True) as response:
                    return response
