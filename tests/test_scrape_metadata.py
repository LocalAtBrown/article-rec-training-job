from datetime import datetime
from unittest import TestCase
from unittest.mock import patch
from typing import Optional

from job.steps.scrape_metadata import scrape_metadata
from sites.sites import Site, Sites


def dummy_validate(site: Site, path: str) -> (None, None, Optional[str]):
    return None, None, None


def dummy_scrape(site: Site, page: None, soup: None) -> dict:
    now = datetime.now()
    return {
        "published_at": dt.isoformat(),
        "title": "Test Title",
        "path": "/article/123456/test-path",
    }


class TestScrapeMetadata(TestCase):
    def setUp(self) -> None:
        self.site = Sites.WCP
        self.paths = [
            "/article/521676/jack-evans-will-pay-2000-a-month-in-latest-ethics-settlement/",
            "/article/530313/douglass-community-land-trust-sw-affordable-housing/",
            "/article/530410/how-to-do-the-last-days-of-summer-right-in-d-c/",
        ]

    @patch("job.steps.scrape_metadata.validate_article", side_effect=dummy_validate)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_recs(self, mock_scrape, mock_validate) -> None:
        pass

    def test_scrape_metadata__existing_recs(self) -> None:
        # article = ArticleFactory.create()
        pass

    def test_scrape_metadata__invalid_recs(self) -> None:
        pass
