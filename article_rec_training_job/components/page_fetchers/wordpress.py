import re
from collections.abc import Callable
from dataclasses import asdict as dataclass_asdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any
from urllib.parse import urlencode, urlparse, urlunparse

from article_rec_db.models import Page
from article_rec_db.models.article import Language
from loguru import logger
from pydantic import HttpUrl


@dataclass(frozen=True)
class QueryParams:
    slug: str | None = None
    lang: Language | None = None
    _fields: list[str] | None = None

    @staticmethod
    def dict_factory(items: list[tuple[str, Any]]) -> dict[str, Any]:
        output = dict()
        for key, value in items:
            # Remove items where value is None
            if value is None:
                continue

            # Convert list values to comma-separated strings
            if isinstance(value, list):
                output[key] = ",".join(value)

        return output

    def __str__(self) -> str:
        params_dict = dataclass_asdict(self, dict_factory=self.dict_factory)
        return urlencode(params_dict)


@dataclass(frozen=True)
class Endpoint:
    scheme: str
    netloc: str
    path: str = "/"
    params: str = ""
    query: QueryParams = QueryParams()
    fragment: str = ""

    def __str__(self) -> str:
        return urlunparse((self.scheme, self.netloc, self.path, self.params, str(self.query), self.fragment))


@dataclass
class BaseFetcher:
    """
    Base WordPress fetcher. Tries to fetch articles from the a given partner site
    using the WordPress REST API v2. If a page is succesfully fetched, creates an
    Article object with relevant metadata to accompany its Page object. If a page
    is not succesfully fetched, merely creates and returns a Page object wrapper.
    """

    # Required page URL prefix that includes protocol and domain name, like https://dallasfreepress.com
    prefix: str
    # Regex pattern to match slug from a path. Make sure to include a named group called "slug".
    # Otherwise, if you want to exert more fine-grained control over how the slug is extracted,
    # you can pass a custom extractor function to the slug_from_path_extractor argument.
    slug_from_path_regex: str | None
    slug_from_path_extractor: Callable[[str], str | None] | None = None

    endpoints = field(init=False, repr=False)

    @cached_property
    def endpoint_posts(self) -> Endpoint:
        """
        API posts endpoint.
        """
        scheme, netloc, _, _, _, _ = urlparse(self.prefix)
        return Endpoint(
            scheme=scheme,
            netloc=netloc,
            path="wp-json/wp/v2/posts",
        )

    @cached_property
    def prefix_pattern(self) -> re.Pattern[str]:
        """
        Regex pattern to match the prefix.
        """
        return re.compile(rf"{self.prefix}")

    @cached_property
    def slug_pattern(self) -> re.Pattern[str] | None:
        """
        Regex pattern to match the slug.
        """
        return re.compile(rf"{self.slug_from_path_regex}") if self.slug_from_path_regex else None

    def remove_urls_invalid_prefix(self, urls: list[str]) -> list[str]:
        """
        Removes URLs with invalid prefix.
        """
        urls_valid = [url for url in urls if self.prefix_pattern.match(url) is not None]
        logger.info(f"Ignored {len(urls) - len(urls_valid)} URLs with invalid prefix")
        return urls_valid

    def infer_language(self, url: str) -> Language:
        """
        Infer content language from a given URL.
        Returns English as default; override this method in subclasses to implement custom site logic.
        """
        return Language.ENGLISH

    def extract_slug(self, url: str) -> str | None:
        """
        Constructs a single API endpoint from a given URL.
        """
        path = urlparse(url).path

        # Extract slug from URL path
        if self.slug_from_path_regex is not None:
            match (slug_match := self.slug_pattern.search(path)):
                case None:
                    slug = None
                case re.Match:
                    slug = slug_match.group("slug")
        elif self.slug_from_path_extractor is not None:
            slug = self.slug_from_path_extractor(path)
        else:
            raise ValueError("Either slug_from_path_regex or slug_from_path_extractor must be provided")

        # If slug is None, warn
        if slug is None:
            logger.warning(
                f"Could not extract slug from path {path}",
                "Make sure your slug_from_path_regex or slug_from_path_extractor works as expected.",
            )

        return slug

    def fetch(self, urls: list[HttpUrl]) -> list[Page]:
        """
        Fetches pages from a given list of URLs.
        """
        # Remove URLs with invalid prefix
        urls = self.remove_urls_invalid_prefix(urls)

        pass

    def post_fetch(self) -> None:
        """
        Post-fetch actions.
        """
        pass
