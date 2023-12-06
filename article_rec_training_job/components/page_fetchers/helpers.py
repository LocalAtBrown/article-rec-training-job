from dataclasses import dataclass
from enum import Enum
from typing import Self
from urllib.parse import urlparse, urlunparse


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
