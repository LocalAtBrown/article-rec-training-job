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
class Globals:
    site: str
    env_postgres_db_url: str


@dataclass
class Components:
    event_fetchers: list[EventFetcher]
    page_fetchers: list[PageFetcher]
    page_writers: list[PageWriter]


@dataclass
class Task:
    type: TaskType
    components: dict[str, str]
    params: dict[str, Any]


@dataclass
class Config:
    job_globals: Globals
    components: Components
    tasks: list[Task]

    # Keep a dict of components for easy access
    _dict_components: dict[str, EventFetcher | PageFetcher | PageWriter] = field(init=False, repr=False)
    # Keep a dict of tasks for easy access
    _dict_tasks: dict[TaskType, Task] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._dict_components = {
            component.type: component
            for component in self.components.event_fetchers + self.components.page_fetchers + self.components.page_writers
        }
        self._dict_tasks = {task.type: task for task in self.tasks}

    def get_component(
        self, component_type: EventFetcherType | PageFetcherType | PageWriterType
    ) -> EventFetcher | PageFetcher | PageWriter | None:
        return self._dict_components.get(component_type)

    def get_task(self, task_type: TaskType) -> Task | None:
        return self._dict_tasks.get(task_type)


def create_config_object(config_dict: dict[str, Any]) -> Config:
    return dataclass_from_dict(data_class=Config, data=config_dict, config=DataclassFromDictConfig(cast=[StrEnum]))
