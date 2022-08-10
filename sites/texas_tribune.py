import logging
import re
from datetime import date
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

import pandas as pd
from bs4 import BeautifulSoup
from requests.models import Response

from sites.helpers import (
    GOOGLE_TAG_MANAGER_RAW_FIELDS,
    ArticleScrapingError,
    ScrapeFailure,
    safe_get,
    transform_data_google_tag_manager,
    validate_response,
    validate_status_code,
)
from sites.site import Site

"""
TT API documentation
https://www.notion.so/a8698dd6527140aaba8acfc29be40aa8?v=d30e06f348e94063ab4f451a345bb0d2&p=209fa6fada864bc0a1555622bb559181
"""

POPULARITY_WINDOW = 7
MAX_ARTICLE_AGE = 10
DOMAIN = "www.texastribune.org"
NAME = "texas-tribune"
FIELDS = GOOGLE_TAG_MANAGER_RAW_FIELDS
TRAINING_PARAMS = {
    "hl": 90,
    "embedding_dim": 100,
    "epochs": 3,
    "tune": False,
    "model": "IMF",
    "loss": "adaptive_hinge",
    "tune_params": ["epochs", "embedding_dim"],
    "tune_ranges": [[5, 11, 2], [160, 360, 100]],
}

SCRAPE_CONFIG = {
    "concurrent_requests": 1,
    "requests_per_second": 2,
}

NON_ARTICLE_PREFIXES = [
    "/districts",
    "/employees",
    "/directory",
    "/newsletters",
    "/states",
    "/search",
    "/departments",
    "/about",
    "/series",
    "/all",
    "/departments",
    "/about",
    "/program",
    "/events",
    "/library",
    "/people",
    "/donate",
    "/topics",
    "/organizations",
    "/jobs",
    "/support-us",
    "/session",
]


class TexasTribune(Site):
    def transform_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return transform_data_google_tag_manager(df=df)

    def extract_external_id(self, path: str) -> Optional[str]:
        for prefix in NON_ARTICLE_PREFIXES:
            if path.startswith(prefix):
                raise ArticleScrapingError(
                    ScrapeFailure.NO_EXTERNAL_ID, path, external_id=None, msg="Skipping non-article path"
                )

        article_url = f"https://{DOMAIN}{path}"
        try:
            page = safe_get(article_url, scrape_config=self.scrape_config)
        except Exception as e:
            raise ArticleScrapingError(
                ScrapeFailure.FETCH_ERROR,
                path,
                external_id=None,
                msg=f"API request failed for {article_url}",
            ) from e
        soup = BeautifulSoup(page.text, features="html.parser")

        html_content = soup.html
        matched = re.search(r"contentID: '\d+'", str(html_content))
        if matched and matched.group(0):
            token = matched.group(0)
            content_id = token.split("'")[1]
            return str(int(content_id))
        else:
            raise ArticleScrapingError(
                ScrapeFailure.NO_EXTERNAL_ID,
                path,
                external_id=None,
                msg="External ID not found",
            )

    def scrape_article_metadata(self, page: Union[Response, dict], external_id: str, path: str) -> dict:
        try:
            api_info = page.json()
        except Exception as e:
            raise ArticleScrapingError(ScrapeFailure.FETCH_ERROR, path, external_id, "Response JSON parse failed") from e
        metadata = self.parse_metadata(api_info, external_id, path)
        return metadata

    def fetch_article(self, external_id: str, path: str) -> Response:
        external_id = int(external_id)  # type: ignore

        api_url = f"https://{DOMAIN}/api/v2/articles/{external_id}"

        try:
            res = safe_get(api_url, scrape_config=self.scrape_config)
        except Exception as e:
            raise ArticleScrapingError(
                ScrapeFailure.FETCH_ERROR, path, external_id, f"Error fetching article url: {api_url}"
            ) from e

        error_msg = validate_response(res, [validate_status_code])
        if error_msg is not None:
            raise ArticleScrapingError(ScrapeFailure.FAILED_SITE_VALIDATION, path, str(external_id), error_msg)

        return res

    def bulk_fetch(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        logging.info(f"Fetching articles from {start_date} to {end_date}")

        API_URL = f"https://{DOMAIN}/api/v2/articles"
        DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
        # texas tribune publishes 5-10 articles per day
        params = {
            "start_date": start_date.strftime(DATE_FORMAT),
            "end_date": end_date.strftime(DATE_FORMAT),
            "limit": 100,
        }
        res = safe_get(API_URL, params=params, scrape_config=self.scrape_config)
        json_res = res.json()

        metadata = [self.parse_metadata(article) for article in json_res["results"]]
        return metadata

    def parse_metadata(
        self, api_info: Dict[str, Any], external_id: Optional[str] = None, path: Optional[str] = None
    ) -> Dict[str, Any]:
        metadata = {}
        parsers = [
            ("title", self.get_title),
            ("published_at", self.get_published_at),
            ("path", self.get_path),
            ("external_id", self.get_external_id),
        ]
        for prop, func in parsers:
            try:
                val = func(api_info)
            except Exception as e:
                if not external_id:
                    external_id = "no_id_obtained"
                if not path:
                    path = "no_path_obtained"
                raise ArticleScrapingError(
                    ScrapeFailure.MALFORMED_RESPONSE, external_id, path, "Error parsing metadata for article"
                ) from e
            metadata[prop] = val

        return metadata

    @staticmethod
    def get_title(res: dict) -> str:
        title = res["headline"]
        return title

    @staticmethod
    def get_published_at(res: dict) -> str:
        # example published_at: '2021-11-12T12:45:35-06:00'
        pub_date = res["pub_date"]
        return pub_date

    @staticmethod
    def get_path(page: dict) -> str:
        # there are times when older articles redirect to an alternate path, for ex:
        # https://washingtoncitypaper.com/food/article/20830693/awardwinning-chef-michel-richard-dies-at-age-68
        return urlparse(page["url"]).path

    @staticmethod
    def get_external_id(page: dict) -> str:
        external_id = page["id"]
        return external_id


TT_SITE = TexasTribune(
    name=NAME,
    fields=FIELDS,
    training_params=TRAINING_PARAMS,
    scrape_config=SCRAPE_CONFIG,
    popularity_window=POPULARITY_WINDOW,
    max_article_age=MAX_ARTICLE_AGE,
)
