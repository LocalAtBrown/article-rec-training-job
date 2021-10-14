from collections import namedtuple

Site = namedtuple(
    "Site",
    ["name", "s3_bucket", "extract_external_id", "scrape_article_metadata", "validate_article"],
)
