import re

from article_rec_db.models.article import Language
from pydantic import HttpUrl

from article_rec_training_job.shared.helpers.urllib import (
    build_url,
    clean_url,
    extract_slug_from_url,
    infer_language_from_url,
    remove_urls_with_invalid_prefix,
)


def test_build_url():
    url = build_url(scheme="https", host="test.com", path="/test", query="a=1&b=2", fragment="test")
    assert url == HttpUrl("https://test.com/test?a=1&b=2#test")


def test_clean_url():
    url = clean_url(HttpUrl("https://test.com/test?a=1&b=2#test"))
    assert url == HttpUrl("https://test.com/test")


def test_infer_language_from_url():
    patterns_language = {
        Language.SPANISH: re.compile(r"^/es/(.+/)*$"),
    }
    url_es = HttpUrl("https://test.com/es/test/")
    url_en = HttpUrl("https://test.com/test/")  # English is the fallback
    assert infer_language_from_url(patterns_language, url_es) == Language.SPANISH
    assert infer_language_from_url(patterns_language, url_en) == Language.ENGLISH


def test_extract_slug_from_url():
    pattern_slug = re.compile(r"^/es/(?P<slug>[a-zA-Z\d\-%]+)/$")
    url = HttpUrl("https://test.com/es/article-slug/")
    assert extract_slug_from_url(pattern_slug, url) == "article-slug"


def test_remove_urls_with_invalid_prefix():
    correct_url_prefix = HttpUrl("https://test.com")
    urls = {
        HttpUrl("https://test.com/test1"),
        HttpUrl("https://test.com/test2"),
        HttpUrl("https://test-wrong.com/test3"),
    }
    assert remove_urls_with_invalid_prefix(correct_url_prefix, urls) == {
        HttpUrl("https://test.com/test1"),
        HttpUrl("https://test.com/test2"),
    }


# Not testing clean_html() for now because it's not terribly important
