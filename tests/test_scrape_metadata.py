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
        "site": site,
    }


class TestScrapeMetadata(BaseTest):
    def setUp(self) -> None:
        self.site = Sites.WCP
        self.external_ids = [
            "521676",
            "530313",
            "530410",
        ]
        super().setUp()

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_valid_recs(self, mock_scrape, mock_validate) -> None:
        article_df = scrape_metadata(self.site, self.external_ids)
        assert len(article_df) == 3
        assert (article_df["site"] == self.site.name).all()

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    @patch("job.steps.scrape_metadata.should_refresh", return_value=True)
    def test_scrape_metadata__existing_valid_recs(
        self, mock_refresh, mock_scrape, mock_validate
    ) -> None:
        for external_id in self.external_ids:
            article = ArticleFactory.create(site=self.site, external_id=external_id)

        article_df = scrape_metadata(self.site, self.external_ids)
        assert len(article_df) == 3
        assert (article_df["site"] == self.site.name).all()

    @patch("job.steps.scrape_metadata.validate_article", return_value=INVALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_invalid_recs(
        self, mock_scrape, mock_validate
    ) -> None:
        article_df = scrape_metadata(self.site, self.external_ids)
        assert len(article_df) == 0

    @patch("job.steps.scrape_metadata.validate_article", return_value=INVALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    @patch("job.steps.scrape_metadata.should_refresh", return_value=True)
    def test_scrape_metadata__existing_invalid_recs(
        self, mock_refresh, mock_scrape, mock_validate
    ) -> None:
        for external_id in self.external_ids:
            article = ArticleFactory.create(site=self.site, external_id=external_id)

        article_df = scrape_metadata(self.site, self.external_ids)
        assert len(article_df) == 0

    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    @patch("job.steps.scrape_metadata.should_refresh", return_value=True)
    def test_scrape_metadata__external_ids(
        self, mock_refresh, mock_scrape, mock_validate
    ) -> None:
        # Unique path string that maps to a duplicate external ID
        # Should be ignored (and not trigger an error)
        self.external_ids.append("530410")

        article_df = scrape_metadata(self.site, self.external_ids)
        assert len(article_df) == 3
