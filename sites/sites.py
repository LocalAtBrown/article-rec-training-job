from collections import namedtuple

from sites.washington_city_paper import (
    extract_external_id as wcp_extract,
    scrape_article_metadata as wcp_scrape,
    validate_article as wcp_validate,
    NAME as wcp_name,
)

Site = namedtuple(
    "Site",
    ["name", "extract_external_id", "scrape_article_metadata", "validate_article"],
)


class Sites:
    WCP = Site(wcp_name, wcp_extract, wcp_scrape, wcp_validate)
