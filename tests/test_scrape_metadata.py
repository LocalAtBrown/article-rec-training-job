from datetime import datetime
from unittest.mock import patch

import pandas as pd
from db.mappings.article import Article
from db.mappings.path import Path

from job.steps.scrape_metadata import scrape_upload_metadata
from sites.helpers import ArticleScrapingError, ScrapeFailure
from sites.site import Site
from sites.sites import Sites
from tests.base import BaseTest
from tests.factories.article import ArticleFactory


def scrape_error(site, article):
    raise ArticleScrapingError(ScrapeFailure.UNKNOWN, article.path, article.external_id)


def safe_scrape_error(site, article):
    raise ArticleScrapingError(ScrapeFailure.FAILED_SITE_VALIDATION, article.path, article.external_id)


VALID_SCRAPE = Article(
    external_id="some ID",
    published_at=datetime.now(),
    site=Sites.WCP.name,
)


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
    @patch(
        "job.steps.scrape_metadata.scrape_article",
        return_value=VALID_SCRAPE,
    )
    def test_scrape_metadata__new_valid_recs(self, _, __) -> None:
        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 3

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch(
        "job.steps.scrape_metadata.scrape_article",
        return_value=VALID_SCRAPE,
    )
    def test_scrape_metadata__existing_valid_recs(self, _, __) -> None:
        for external_id in self.external_ids:
            ArticleFactory.create(site=self.site.name, external_id=external_id)
        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 3

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch(
        "job.steps.scrape_metadata.scrape_article",
        return_value=None,
        side_effect=scrape_error,
    )
    def test_scrape_metadata__new_invalid_recs(self, _, __) -> None:
        scrape_upload_metadata(self.site, dts=[])
        # No new articles created
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 0
        # No exclude list entries created
        res = Path.select().where(Path.site == self.site.name)
        assert len(res) == 0

    @patch("job.steps.warehouse.get_paths_to_update", return_value=EXISTING_VALID)
    @patch(
        "job.steps.scrape_metadata.scrape_article",
        return_value=None,
        side_effect=scrape_error,
    )
    def test_scrape_metadata__existing_invalid_recs(self, _, __) -> None:
        for external_id in self.external_ids:
            ArticleFactory.create(site=self.site.name, external_id=external_id)

        scrape_upload_metadata(self.site, dts=[])
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 0

    @patch("job.steps.warehouse.get_paths_to_update", return_value=NEW_VALID)
    @patch(
        "job.steps.scrape_metadata.scrape_article",
        return_value=None,
        side_effect=safe_scrape_error,
    )
    def test_scrape_metadata__new_invalid_safe_recs(self, _, __) -> None:
        scrape_upload_metadata(self.site, dts=[])
        # No new articles created
        res = Article.select().where(Article.site == self.site.name)
        assert len(res) == 0
        # Exclude list entries created
        res = Path.select().where(Path.site == self.site.name)
        assert len(res) == 3
