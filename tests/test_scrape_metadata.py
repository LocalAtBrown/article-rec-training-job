from datetime import datetime
from unittest import TestCase
from unittest.mock import patch
from typing import Optional

from job.steps.scrape_metadata import scrape_metadata
from sites.sites import Site, Sites
from tests.base import BaseTest
from tests.factories.article import ArticleFactory


VALID_RES = (None, None, None)
INVALID_RES = (None, None, "Test invalid message")


def dummy_scrape(site: Site, page: None, soup: None) -> dict:
    now = datetime.now()
    return {
        "published_at": str(now),
        "title": "Test Title",
        "path": "/article/123456/test-path",
    }


class TestScrapeMetadata(BaseTest):
    def setUp(self) -> None:
        self.site = Sites.WCP
        self.paths = [
            "/article/521676/jack-evans-will-pay-2000-a-month-in-latest-ethics-settlement/",
            "/article/530313/douglass-community-land-trust-sw-affordable-housing/",
            "/article/530410/how-to-do-the-last-days-of-summer-right-in-d-c/",
        ]
        super().setUp()

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_valid_recs(self, mock_scrape, mock_validate) -> None:
        article_df = scrape_metadata(self.site, self.paths)
        assert len(article_df) == 3

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    @patch("job.steps.scrape_metadata.should_refresh", return_value=True)
    def test_scrape_metadata__existing_valid_recs(
        self, mock_refresh, mock_scrape, mock_validate
    ) -> None:
        for path in self.paths:
            existing_id = self.site.extract_external_id(path)
            article = ArticleFactory.create(external_id=existing_id)

        article_df = scrape_metadata(self.site, self.paths)
        assert len(article_df) == 3

    @patch("job.steps.scrape_metadata.validate_article", return_value=INVALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_invalid_recs(
        self, mock_scrape, mock_validate
    ) -> None:
        article_df = scrape_metadata(self.site, self.paths)
        assert len(article_df) == 0

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    @patch("job.steps.scrape_metadata.should_refresh", return_value=True)
    def test_scrape_metadata__duplicate_paths(
        self, mock_refresh, mock_scrape, mock_validate
    ) -> None:
        # Unique path string that maps to a duplicate external ID
        # Should be ignored (and not trigger an error)
        self.paths.append(
            "/v/s/article/530410/how-to-do-the-last-days-of-summer-right-in-d-c/"
        )

        article_df = scrape_metadata(self.site, self.paths)
        assert len(article_df) == 3
