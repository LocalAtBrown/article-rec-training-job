from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from dacite import Config as DataclassFromDictConfig
from dacite import from_dict as dataclass_from_dict


class EventFetcherType(StrEnum):
    GA4_BASE = "ga4_base"


class PageFetcherType(StrEnum):
    WP_BASE = "wp_base"


class PageWriterType(StrEnum):
    POSTGRES_BASE = "postgres_base"


class TaskType(StrEnum):
    UPDATE_PAGES = "update_pages"


@dataclass
class EventFetcher:
    type: EventFetcherType
    params: dict[str, Any]


@dataclass
class PageFetcher:
    type: PageFetcherType
    params: dict[str, Any]


@dataclass
class PageWriter:
    type: PageWriterType
    params: dict[str, Any]


@dataclass
class Components:
    event_fetchers: list[EventFetcher]
    page_fetchers: list[PageFetcher]
    page_writers: list[PageWriter]

    # Keep a dict of components for easy access
    _dict_components: dict[str, EventFetcher | PageFetcher | PageWriter] = field(init=False, repr=False)

    def __post_init__(self):
        self._dict_components = {
            component.type: component for component in self.event_fetchers + self.page_fetchers + self.page_writers
        }

    def __getitem__(self, key: str) -> EventFetcher | PageFetcher | PageWriter:
        return self._dict_components[key]


@dataclass
class Task:
    type: TaskType
    components: dict[str, str]
    params: dict[str, Any]


@dataclass
class Config:
    site: str
    components: Components
    tasks: list[Task]


def create_config_object(config_dict: dict[str, Any]) -> Config:
    return dataclass_from_dict(data_class=Config, data=config_dict, config=DataclassFromDictConfig(cast=[StrEnum]))
