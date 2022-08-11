from collections import namedtuple

Site = namedtuple(
    "Site",
    [
        "name",
        "fields",
        "training_params",
        "scrape_config",
        "transform_raw_data",
        "extract_external_id",
        "scrape_article_metadata",
        "fetch_article",
        "bulk_fetch",
        "bulk_fetch_by_article_id",
        "get_article_text",
        "popularity_window",
        "max_article_age",
    ],
)


def get_bucket_name(site: Site):
    return f"lnl-snowplow-{site.name}"
