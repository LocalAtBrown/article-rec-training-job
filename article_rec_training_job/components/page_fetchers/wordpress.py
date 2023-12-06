import re
from collections.abc import Callable
from dataclasses import asdict as dataclass_asdict
from dataclasses import dataclass, field
from functools import cached_property
from typing import Any, Self
from urllib.parse import urlencode, urlparse, urlunparse

from article_rec_db.models import Page
from article_rec_db.models.article import Language
from loguru import logger
from pydantic import HttpUrl

from article_rec_training_job.shared.types.page_fetchers import Output


@dataclass(frozen=True)
class APIQueryParams:
    """
    Dataclass wrapper for WordPress REST API v2 query parameters.
    """

    # Add more query parameters as needed
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


@dataclass(eq=True, frozen=True)
class URL:
    """
    Dataclass wrapper for URL strings, use for both page URLs and API endpoints.
    Hashable, so can be used as dict keys or set entries.
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

    urls_to_update: set[URL] = field(init=False, repr=False)
    slugs: dict[URL, str] = field(init=False, repr=False)

    @cached_property
    def endpoint_posts(self) -> URL:
        """
        API posts endpoint.
        """
        scheme, netloc, _, _, _, _ = urlparse(self.prefix)
        return URL(
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

    def infer_language(self, url: URL) -> Language:
        """
        Infer content language from a given URL.
        Returns English as default; override this method in subclasses to implement custom site logic.
        """
        return Language.ENGLISH

    def extract_slug(self, url: URL) -> str | None:
        """
        Constructs a single API endpoint from a given URL.
        """
        path = url.path

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

    def preprocess_urls(self, urls: set[str]) -> set[URL]:
        """
        Preprocesses a given set of URLs.
        """
        # Remove URLs with invalid prefix
        urls = {url for url in urls if self.prefix_pattern.match(url) is not None}

        # Remove params, query, and fragment from each URL; remove duplicates afterward
        url_objects = {URL.create_cleaned_from_string(url) for url in urls}

        return url_objects

    def fetch(self, urls: set[HttpUrl]) -> Output:
        """
        Fetches pages from a given list of URLs.
        """
        # Preprocess URLs
        self.urls_to_update = self.preprocess_urls(urls)

        # Each URL corresponds to a page
        pages = [Page(url=url.convert_to_string()) for url in self.urls_to_update]

        # Extract slugs from URLs
        self.slugs = {url: self.extract_slug(url) for url in self.urls_to_update}

        # TODO: Fetch articles where slugs are not None

        return Output(pages=pages)

    def post_fetch(self) -> None:
        """
        Post-fetch actions.
        """
        pass
