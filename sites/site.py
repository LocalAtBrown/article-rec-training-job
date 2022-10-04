from abc import abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from requests.models import Response

from sites.config import SiteConfig
from sites.singleton import SingletonABCMeta
from sites.strategy import Strategy


@dataclass
class Site(metaclass=SingletonABCMeta):
    name: str
    # Main recs-generating approach, e.g., CF or SS
    strategy: Strategy
    # Backup/default recs-generating approach, e.g., popularity
    strategy_fallback: Strategy
    config: SiteConfig

    def get_bucket_name(self):
        return f"lnl-snowplow-{self.name}"

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

    @abstractmethod
    def bulk_fetch_by_external_id(self, external_ids: List[str]) -> List[Dict[str, Any]]:
        pass

    @staticmethod
    @abstractmethod
    def transform_raw_data(df: pd.DataFrame) -> pd.DataFrame:
        pass

    @staticmethod
    @abstractmethod
    def get_article_text(metadata: Dict[str, Any]) -> str:
        pass
