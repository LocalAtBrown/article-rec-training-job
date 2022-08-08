from abc import ABC, abstractmethod
from collections import namedtuple
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from requests.models import Response

Site = namedtuple(
    "Site",
    [
        "name",  # property, string
        "fields",  # property, dict
        "training_params",  # property, dict (class candidate)
        "scrape_config",  # property, dict (class candidate)
        "transform_raw_data",  # function, (df) -> df
        "extract_external_id",  # function, (path: str) -> optional(str)
        "scrape_article_metadata",  # function, (page, id, path) -> dict
        "fetch_article",  # function, (id, path) -> response
        "bulk_fetch",  # (start_date, end_date) -> list[dict[str???]]
        "popularity_window",  # property, int
        "max_article_age",  # property, int
    ],
)


@dataclass
class NewSite(ABC):
    name: str
    fields: dict  # needs better name
    training_params: dict  # should define with more specifics unless it needs to be flexible
    scrape_config: dict  # should define with more specifics unless it needs to be flexible
    popularity_window: int  # what does this mean to a human? days?
    max_article_age: int  # what does this mean to a human? days?

    def get_bucket_name(self):
        return f"lnl-snowplow-{self.name}"

    @abstractmethod
    def transform_raw_data(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    @abstractmethod
    def extract_external_id(self, path: str) -> Optional[str]:
        pass

    @abstractmethod
    def scrape_article_metadata(self, page: Union[Response, dict], external_id: str, path: str) -> dict:
        pass

    @abstractmethod
    def fetch_article(self, external_id: str, path: str) -> Response:
        pass

    @abstractmethod
    def bulk_fetch(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        pass


def get_bucket_name(site: Site):
    return f"lnl-snowplow-{site.name}"
