from pydantic import HttpUrl

from article_rec_training_job.components.page_fetchers.helpers import (
    build_url,
    clean_url,
)


def test_build_url():
    url = build_url(scheme="https", host="test.com", path="/test", query="a=1&b=2", fragment="test")
    assert url == HttpUrl("https://test.com/test?a=1&b=2#test")


def test_clean_url():
    url = clean_url(HttpUrl("https://test.com/test?a=1&b=2#test"))
    assert url == HttpUrl("https://test.com/test")


# Not testing clean_html() right now because it's not terribly important
