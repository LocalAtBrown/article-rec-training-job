from collections import namedtuple

Site = namedtuple(
    "Site",
    [
        "name",
        "fields",
        "training_params",
        "transform_raw_data",
        "extract_external_id",
        "scrape_article_metadata",
        "fetch_article",
        "bulk_fetch",
    ],
)


def get_bucket_name(site: Site):
    return f"lnl-snowplow-{site.name}"
