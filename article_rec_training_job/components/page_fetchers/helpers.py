import re
from enum import Enum
from urllib.parse import urlunparse

import nh3
from aiohttp import ClientResponse, ClientSession
from article_rec_db.models.article import Language
from loguru import logger
from pydantic import HttpUrl
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    stop_after_attempt,
    wait_random_exponential,
)


class HTMLAllowedEntities(Enum):
    TAGS = {"a", "img", "video", "h1", "h2", "h3", "strong", "em", "p", "ul", "ol", "li", "br", "sub", "sup", "hr"}
    ATTRIBUTES = {
        "a": {"href", "name", "target", "title", "id"},
        "img": {"src", "alt", "title"},
        "video": {"src", "alt", "title"},
    }


def build_url(scheme: str, host: str, path: str, query: str = "", fragment: str = "") -> HttpUrl:
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
    url_new = build_url(scheme=url.scheme, host=url.host, path=url.path)  # type: ignore
    return url_new


def infer_language_from_url(patterns_language: dict[Language, re.Pattern[str]], url: HttpUrl) -> Language:
    """
    Infer content language from a given URL.
    Returns English as a fallback.
    """
    path = url.path
    for language, pattern in patterns_language.items():
        if pattern.fullmatch(path) is not None:  # type: ignore
            return language
    return Language.ENGLISH


def extract_slug_from_url(pattern_slug: re.Pattern[str], url: HttpUrl) -> str | None:
    """
    Grabs the slug from a given URL.
    """
    path = url.path

    slug_match = pattern_slug.fullmatch(path)  # type: ignore
    if slug_match is None:
        logger.info(
            f"Could not extract slug from path {path}. It will be considered a non-article page.",
        )
        return None

    return slug_match.group("slug")


def remove_urls_with_invalid_prefix(correct_url_prefix: HttpUrl, urls: set[HttpUrl]) -> set[HttpUrl]:
    """
    Removes URLs with invalid prefix.
    """
    return {url for url in urls if url.scheme == correct_url_prefix.scheme and url.host == correct_url_prefix.host}


def clean_html(html: str) -> str:
    """
    Cleans HTML by removing all tags and attributes that are not explicitly allowed.
    """
    return nh3.clean(html, tags=HTMLAllowedEntities.TAGS.value, attributes=HTMLAllowedEntities.ATTRIBUTES.value)


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
