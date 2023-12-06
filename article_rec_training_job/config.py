from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

from dacite import Config as DataclassFromDictConfig
from dacite import from_dict as dataclass_from_dict


class EventFetcherType(StrEnum):
    GA4_BASE = "ga4_base"


@dataclass
class EventFetcher:
    type: EventFetcherType
    params: dict


@dataclass
class TaskUpdatePages:
    execution_timestamp_utc: datetime | None
    event_fetcher: EventFetcher


@dataclass
class TaskCreateRecommendations:
    execution_timestamp_utc: datetime | None
    event_fetcher: EventFetcher


@dataclass
class Tasks:
    update_pages: TaskUpdatePages | None
    create_recommendations: TaskCreateRecommendations | None


@dataclass
class Config:
    site: str
    tasks: Tasks


def create_config_object(config_dict: dict) -> Config:
    return dataclass_from_dict(data_class=Config, data=config_dict, config=DataclassFromDictConfig(cast=[StrEnum]))