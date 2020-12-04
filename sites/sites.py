from collections import namedtuple

from sites.washington_city_paper import (
    extract_external_id as wcp_extract,
    scrape_article_metadata as wcp_scrape,
)

Site = namedtuple("Site", ["extract_external_id", "scrape_article_metadata"])


class Sites:
    WCP = Site(wcp_extract, wcp_scrape)
