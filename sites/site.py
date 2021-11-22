from collections import namedtuple

Site = namedtuple(
    "Site",
    [
        "name",
        "FIELDS",
        "extract_external_id",
        "scrape_article_metadata",
        "validate_article",
    ],
)


def get_bucket_name(site: Site):
    return f"lnl-snowplow-{site.name}"
