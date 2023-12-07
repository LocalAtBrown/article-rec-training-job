from dataclasses import dataclass
from enum import Enum
from typing import Self
from urllib.parse import urlparse, urlunparse

import nh3
from aiohttp import ClientResponse, ClientSession
from loguru import logger
from tenacity import AsyncRetrying, stop_after_attempt, wait_random_exponential


class HTMLAllowedEntities(Enum):
    TAGS = {"a", "img", "video", "h1", "h2", "h3", "strong", "em", "p", "ul", "ol", "li", "br", "sub", "sup", "hr"}
    ATTRIBUTES = {
        "a": {"href", "name", "target", "title", "id"},
        "img": {"src", "alt", "title"},
        "video": {"src", "alt", "title"},
    }


@dataclass(eq=True, frozen=True)
class URL:
    """
    Dataclass wrapper for URL strings, use for both page URLs and API endpoints.
    Hashable, so can be used as dict keys or set entries.

    We use this instead of Pydantic's HttpUrl because in addition to validation
    and building URLs from strings, we want to be able to build a URL by specifying
    the parts separately, clean URLs by removing query parameters and fragments,
    and make them hashable.
    """

    scheme: str
    netloc: str
    path: str = "/"
    params: str = ""
    query: str = ""
    fragment: str = ""

    @classmethod
    def create_from_string(cls, url: str) -> Self:
        scheme, netloc, path, params, query, fragment = urlparse(url)
        return cls(scheme, netloc, path, params, query, fragment)

    @classmethod
    def create_cleaned_from_string(cls, url: str) -> Self:
        scheme, netloc, path, _, _, _ = urlparse(url)
        return cls(scheme, netloc, path)

    def convert_to_string(self) -> str:
        return urlunparse((self.scheme, self.netloc, self.path, self.params, self.query, self.fragment))

    def convert_to_clean_string(self) -> str:
        return urlunparse((self.scheme, self.netloc, self.path, "", "", ""))

    def __str__(self) -> str:
        return self.convert_to_string()


def clean_html(html: str) -> str:
    """
    Cleans HTML by removing all tags and attributes that are not explicitly allowed.
    """
    return nh3.clean(html, tags=HTMLAllowedEntities.TAGS.value, attributes=HTMLAllowedEntities.ATTRIBUTES.value)


async def request(url: URL, maximum_attempts: int, maximum_backoff: int) -> ClientResponse:
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
                async with session.get(url.convert_to_string(), raise_for_status=True) as response:
                    return response
