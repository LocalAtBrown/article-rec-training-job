import asyncio
import re
from dataclasses import asdict as dataclass_asdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import Any
from urllib.parse import urlencode

from aiohttp import ClientResponseError
from article_rec_db.models import Article, Page
from article_rec_db.models.article import Language
from loguru import logger
from pydantic import Field as PydanticField
from pydantic import HttpUrl, field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass

from article_rec_training_job.components.page_fetchers.helpers import (
    build_url,
    clean_html,
    clean_url,
    request,
)
from article_rec_training_job.shared.helpers.time import get_elapsed_time


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
            if value is None:
                # Remove items where value is None
                continue
            elif isinstance(value, list):
                # Convert list values to comma-separated strings
                output[key] = ",".join(value)
            else:
                output[key] = value

        return output

    def __str__(self) -> str:
        params_dict = dataclass_asdict(self, dict_factory=self.dict_factory)
        return urlencode(params_dict)


@pydantic_dataclass
class BaseFetcher:
    """
    Base WordPress fetcher. Tries to fetch articles from the a given partner site
    using the WordPress REST API v2. If a page is succesfully fetched, creates an
    Article object with relevant metadata to accompany its Page object. If a page
    is not succesfully fetched, merely creates Page object without an Article object
    inside it. Returns a list of Page objects.
    """

    # Required site name
    site_name: str
    # Regex pattern to match slug from a path. Need to include a named group called "slug".
    slug_from_path_regex: str
    # WordPress ID of tag to signify in-house content
    tag_id_republished_content: int | None
    # Maximum number of retries for a request
    request_maximum_attempts: int
    # Maximum backoff before retrying a request, in seconds
    request_maximum_backoff: int
    # Required page URL prefix that includes protocol and domain name, like https://dallasfreepress.com
    url_prefix_str: str = PydanticField(pattern=r"^https?://[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")
    # Regex patterns to identify content language from a path.
    language_from_path_regex: dict[Language, str] = field(default_factory=dict)

    urls_to_update: set[HttpUrl] = field(init=False, repr=False)
    slugs: dict[HttpUrl, str | None] = field(init=False, repr=False)
    time_taken_to_fetch_pages: float = field(init=False, repr=False)
    num_articles_fetched: int = field(init=False, repr=False)

    @field_validator("slug_from_path_regex", mode="after")
    def slug_regex_must_have_slug_group(cls, value: str) -> str:
        """
        Validates that the slug regex has a named group called "slug".
        """
        if "slug" not in re.compile(value).groupindex:
            raise ValueError("Slug regex must have a named group called 'slug'")
        return value

    @field_validator("slug_from_path_regex", mode="after")
    def slug_regex_must_cover_entire_path(cls, value: str) -> str:
        """
        Validates that the slug regex is meant to work on the entire path
        using re.Pattern.fullmatch().
        """
        if value[0] != "^" or value[-1] != "$":
            raise ValueError("Slug regex must start with ^ and end with $, i.e., it must cover the entire path")
        return value

    @field_validator("language_from_path_regex", mode="after")
    def language_regexes_must_cover_entire_path(cls, value: dict[Language, str]) -> dict[Language, str]:
        """
        Validates that the language regexes are meant to work on the entire path
        using re.Pattern.fullmatch().
        """
        for language, pattern in value.items():
            if pattern[0] != "^" or pattern[-1] != "$":
                raise ValueError(
                    f"Language regex for {language.value} must start with ^ and end with $, i.e., it must cover the entire path"
                )
        return value

    @cached_property
    def url_prefix(self) -> HttpUrl:
        """
        Regex pattern to match the prefix.
        """
        # Validation step ensures self.url_prefix_str only has scheme and host
        return HttpUrl(self.url_prefix_str)

    @cached_property
    def endpoint_posts(self) -> HttpUrl:
        """
        API posts endpoint.
        """
        return build_url(
            scheme=self.url_prefix.scheme,
            host=self.url_prefix.host,  # type: ignore
            path="/wp-json/wp/v2/posts/",
        )

    @cached_property
    def pattern_slug(self) -> re.Pattern[str]:
        """
        Regex pattern to match the slug.
        """
        return re.compile(rf"{self.slug_from_path_regex}")

    @cached_property
    def patterns_language(self) -> dict[Language, re.Pattern[str]]:
        """
        Regex patterns to match the language.
        """
        return {language: re.compile(rf"{pattern}") for language, pattern in self.language_from_path_regex.items()}

    def infer_language(self, url: HttpUrl) -> Language:
        """
        Infer content language from a given URL.
        Returns English as a fallback.
        """
        path = url.path
        for language, pattern in self.patterns_language.items():
            if pattern.fullmatch(path) is not None:  # type: ignore
                return language
        return Language.ENGLISH

    def extract_slug(self, url: HttpUrl) -> str | None:
        """
        Grabs the slug from a given URL.
        """
        path = url.path

        slug_match = self.pattern_slug.fullmatch(path)  # type: ignore
        if slug_match is None:
            logger.info(
                f"Could not extract slug from path {path}. It will be considered a non-article page.",
            )
            return None

        return slug_match.group("slug")

    def preprocess_urls(self, urls: set[HttpUrl]) -> set[HttpUrl]:
        """
        Preprocesses a given set of URLs.
        """
        # Remove URLs with invalid prefix
        urls = {url for url in urls if url.scheme == self.url_prefix.scheme and url.host == self.url_prefix.host}

        # Remove params, query, and fragment from each URL; remove duplicates afterward
        url_objects = {clean_url(url) for url in urls}

        return url_objects

    async def fetch_page(self, url: HttpUrl) -> Page:
        """
        Fetches a single article from a given URL. Returns a Page object
        containing an Article object if the article is successfully fetched,
        or a Page object without an Article object if the article is not
        or if it doesn't have a slug.
        """
        slug = self.slugs[url]
        page_without_article = Page(url=url)

        # Don't go any further if slug is None, i.e., page is not an article
        if slug is None:
            return page_without_article

        # Infer language
        language = self.infer_language(url)

        # Construct API endpoint
        fields = [
            "id",
            "date_gmt",
            "modified_gmt",
            "slug",
            "status",
            "type",
            "link",
            "title",
            "content",
            "excerpt",
            "tags",
        ]
        query = APIQueryParams(slug=slug, lang=language, _fields=fields)
        endpoint = build_url(
            scheme=self.endpoint_posts.scheme,
            host=self.endpoint_posts.host,  # type: ignore
            path=self.endpoint_posts.path,  # type: ignore
            query=str(query),
        )

        # Fetch article
        logger.info(f"Requesting WordPress API for slug {slug}")
        try:
            response = await request(endpoint, self.request_maximum_attempts, self.request_maximum_backoff)
        except ClientResponseError as e:
            logger.warning(
                f"Request to WordPress API for slug {slug} failed because response code indicated an error: {e}"
            )
            return page_without_article
        except Exception:
            logger.opt(exception=True).warning(
                f"Request to WordPress API for slug {slug} failed because of an unknown error. Traceback:"
            )
            return page_without_article

        # Check that response is actually what we want.
        # If so, build Article object and return it inside the Page object.
        match (data := await response.json()):
            # If response is a list of one dict with
            # - key "slug" that matches the slug we want
            # - key "type" that is "post"
            # - key "link" that matches the URL we want (need this because sometimes the slug is not enough)
            # - key "status" indicating that the post is published, not draft or pending
            case [{"slug": _slug, "type": "post", "link": _link, "status": "publish"}] if (
                _slug == slug and _link == str(url)
            ):
                datum = data[0]
                article = Article(
                    site=self.site_name,
                    id_in_site=str(datum["id"]),
                    title=datum["title"]["rendered"],
                    description=clean_html(datum["excerpt"]["rendered"]),
                    content=clean_html(datum["content"]["rendered"]),
                    site_published_at=datetime.fromisoformat(datum["date_gmt"]),
                    site_updated_at=datetime.fromisoformat(datum["modified_gmt"]),
                    language=language,
                    is_in_house_content=self.tag_id_republished_content not in datum["tags"],
                )
                return Page(url=url, article=article)
            case _:
                logger.warning(
                    f"Request to WordPress API for slug {slug} successfully returned, but response is not what we want",
                )
                return page_without_article

    def fetch(self, urls: set[HttpUrl]) -> list[Page]:
        """
        Fetches pages from a given list of URLs.
        """

        async def fetch_pages_async(urls: set[HttpUrl]) -> list[Page]:
            return await asyncio.gather(*[self.fetch_page(url) for url in urls])

        @get_elapsed_time
        def fetch_pages(urls: set[HttpUrl]) -> list[Page]:
            return asyncio.run(fetch_pages_async(urls))

        # Preprocess URLs
        self.urls_to_update = self.preprocess_urls(urls)

        # Extract slugs from URLs
        self.slugs = {url: self.extract_slug(url) for url in self.urls_to_update}

        # Fetch articles
        self.time_taken_to_fetch_pages, pages = fetch_pages(self.urls_to_update)

        # Count articles
        self.num_articles_fetched = sum([page.article is not None for page in pages])

        return pages

    def post_fetch(self) -> None:
        """
        Post-fetch actions.
        """
        num_articles = len(self.urls_to_update)
        num_slugs = len([slug for slug in self.slugs.values() if slug is not None])
        average_article_latency = (
            self.time_taken_to_fetch_pages / self.num_articles_fetched if self.num_articles_fetched > 0 else None
        )

        logger.info(f"{num_articles} URLs passed preprocessing")
        logger.info(f"{num_slugs} slugs were successfully extracted from URLs")
        logger.info(f"Fetching took {self.time_taken_to_fetch_pages:.3f} seconds")

        if average_article_latency is not None:
            logger.info(f"Fetching an article took on average {average_article_latency:.3f} seconds")
