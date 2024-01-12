from enum import Enum

import nh3


class HTMLAllowedEntities(Enum):
    TAGS = {"a", "img", "video", "h1", "h2", "h3", "strong", "em", "p", "ul", "ol", "li", "br", "sub", "sup", "hr"}
    ATTRIBUTES = {
        "a": {"href", "name", "target", "title", "id"},
        "img": {"src", "alt", "title"},
        "video": {"src", "alt", "title"},
    }


def clean_html(html: str) -> str:
    """
    Cleans HTML by removing all tags and attributes that are not explicitly allowed.
    """
    return nh3.clean(html, tags=HTMLAllowedEntities.TAGS.value, attributes=HTMLAllowedEntities.ATTRIBUTES.value)
