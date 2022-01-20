from datetime import datetime
from unittest import TestCase
from unittest.mock import patch
from typing import Optional

import pandas as pd
from db.mappings.article import Article

from job.steps.scrape_metadata import scrape_upload_metadata
from sites.site import Site
from sites.sites import Sites
from tests.base import BaseTest
from tests.factories.article import ArticleFactory


VALID_RES = (None, None, None)
INVALID_RES = (None, None, "Test invalid message")

NEW_VALID = pd.DataFrame(
    {
        "landing_page_path": [
            "/article/521676/a",
            "/article/530313/b",
            "/article/530410/c",
        ],
        "external_id": [
            None,
            None,
            None,
        ],
    }
)

EXISTING_VALID = pd.DataFrame(
    {
        "landing_page_path": [
            "/article/521676/a",
            "/article/530313/b",
            "/article/530410/c",
        ],
        "external_id": [
            "521676",
            "530313",
            "530410",
        ],
    }
)


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

    @patch("job.steps.warehouse.get_paths_to_update", return_value=NEW_VALID)
    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_valid_recs(self, _, __, ___) -> None:
        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 3

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch("job.steps.scrape_metadata.validate_article", return_value=VALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__existing_valid_recs(self, _, __, ___) -> None:
        for external_id in self.external_ids:
            article = ArticleFactory.create(
                site=self.site.name, external_id=external_id
            )
        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 3

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch("job.steps.scrape_metadata.validate_article", return_value=INVALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__new_invalid_recs(self, _, __, ___) -> None:
        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 0

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch("job.steps.scrape_metadata.validate_article", return_value=INVALID_RES)
    @patch(
        "job.steps.scrape_metadata.scrape_article_metadata", side_effect=dummy_scrape
    )
    def test_scrape_metadata__existing_invalid_recs(self, _, __, ___) -> None:
        for external_id in self.external_ids:
            article = ArticleFactory.create(
                site=self.site.name, external_id=external_id
            )

        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 0
