import asyncio
import re
from dataclasses import asdict as dataclass_asdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import Any
from urllib.parse import urlencode, urlparse

from aiohttp import ClientResponseError
from article_rec_db.models import Article, Page
from article_rec_db.models.article import Language
from loguru import logger
from pydantic import HttpUrl

from article_rec_training_job.components.page_fetchers.helpers import (
    URL,
    clean_html,
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


@dataclass
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
    # Required page URL prefix that includes protocol and domain name, like https://dallasfreepress.com
    url_prefix: str
    # Regex pattern to match slug from a path. Need to include a named group called "slug".
    slug_from_path_regex: str
    # WordPress ID of tag to signify in-house content
    tag_id_in_house_content: int | None = None
    # Maximum number of retries for a request
    request_maximum_attempts: int = 10
    # Maximum backoff before retrying a request, in seconds
    request_maximum_backoff: int = 60

    urls_to_update: set[URL] = field(init=False, repr=False)
    slugs: dict[URL, str] = field(init=False, repr=False)
    time_taken_to_fetch_pages: float = field(init=False, repr=False)
    num_articles_fetched: int = field(init=False, repr=False)

    @cached_property
    def endpoint_posts(self) -> URL:
        """
        API posts endpoint.
        """
        scheme, netloc, _, _, _, _ = urlparse(self.url_prefix)
        return URL(
            scheme=scheme,
            netloc=netloc,
            path="wp-json/wp/v2/posts",
        )

    @cached_property
    def pattern_prefix(self) -> re.Pattern[str]:
        """
        Regex pattern to match the prefix.
        """
        return re.compile(rf"{self.url_prefix}")

    @cached_property
    def pattern_slug(self) -> re.Pattern[str]:
        """
        Regex pattern to match the slug.
        """
        return re.compile(rf"{self.slug_from_path_regex}")

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
        match (slug_match := self.pattern_slug.search(path)):
            case None:
                slug = None
            case re.Match:
                slug = slug_match.group("slug")

        # If slug is None, warn
        if slug is None:
            logger.info(
                f"Could not extract slug from path {path}. It will be considered a non-article page.",
            )

        return slug

    def preprocess_urls(self, urls: set[str]) -> set[URL]:
        """
        Preprocesses a given set of URLs.
        """
        # Remove URLs with invalid prefix
        urls = {url for url in urls if self.url_pattern_prefix.match(url) is not None}

        # Remove params, query, and fragment from each URL; remove duplicates afterward
        url_objects = {URL.create_cleaned_from_string(url) for url in urls}

        return url_objects

    async def fetch_page(self, url: URL) -> Page:
        """
        Fetches a single article from a given URL. Returns a Page object
        containing an Article object if the article is successfully fetched,
        or a Page object without an Article object if the article is not
        or if it doesn't have a slug.
        """
        slug = self.slugs[url]
        page_without_article = Page(url=url.convert_to_string())

        # Don't go any further if slug is None, i.e., page is not an article
        if slug is None:
            return page_without_article

        # Infer language
        language = self.infer_language(url)

        # Construct API endpoint
        fields = ["id", "date_gmt", "modified_gmt", "slug", "status", "type", "title", "content", "excerpt", "tags"]
        endpoint = URL(
            scheme=self.endpoint_posts.scheme,
            netloc=self.endpoint_posts.netloc,
            path=self.endpoint_posts.path,
            query=str(
                APIQueryParams(
                    slug=slug,
                    lang=language,
                    _fields=fields,
                )
            ),
        )

        # Fetch article
        logger.info(f"Requesting WordPress API for {slug}")
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
        else:
            data = await response.json()

        # Check that response is actually what we want.
        # If so, build Article object and return it inside the Page object.
        match data:
            # If response is a list of one dict with these fields
            case [{"slug": slug, "type": "post"}]:
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
                    is_in_house_content=self.tag_id_in_house_content in datum["tags"],
                )
                return Page(url=url.convert_to_string(), article=[article])
            case _:
                logger.warning(
                    f"Request to WordPress API for slug {slug} successfully returned, but response is not what we want",
                )
                return page_without_article

    def fetch(self, urls: set[HttpUrl]) -> list[Page]:
        """
        Fetches pages from a given list of URLs.
        """

        @get_elapsed_time
        def fetch_pages() -> list[Page]:
            return asyncio.run(asyncio.gather(*(self.fetch_page(url) for url in self.urls_to_update)))

        # Preprocess URLs
        self.urls_to_update = self.preprocess_urls(urls)

        # Extract slugs from URLs
        self.slugs = {url: self.extract_slug(url) for url in self.urls_to_update}

        # Fetch articles
        self.time_taken_to_fetch_pages, pages = asyncio.run(
            asyncio.gather(*(self.fetch_page(url) for url in self.urls_to_update))
        )

        # Count articles
        self.num_articles_fetched = sum([len(page.article) == 1 for page in pages])

        return pages

    def post_fetch(self) -> None:
        """
        Post-fetch actions.
        """
        num_articles = len(self.urls_to_update)
        num_slugs = len([slug for slug in self.slugs.values() if slug is not None])
        average_article_latency = self.time_taken_to_fetch_pages / self.num_articles_fetched

        logger.info(f"{num_articles} URLs passed preprocessing, corresponding to {num_articles} pages")
        logger.info(f"{num_slugs} slugs were successfully extracted from URLs")
        logger.info(f"{self.num_articles_fetched} articles out of {num_articles} were successfully fetched")
        logger.info(f"Fetching took {self.time_taken_to_fetch_pages:.3f} seconds")
        logger.info(f"Fetching an article took on average {average_article_latency:.3f} seconds")
