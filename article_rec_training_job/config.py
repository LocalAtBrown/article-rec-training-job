from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, TypeAlias

from dacite import Config as DataclassFromDictConfig
from dacite import from_dict as dataclass_from_dict


class EventFetcherType(StrEnum):
    GA4_BASE = "ga4_base"


class PageFetcherType(StrEnum):
    WP_BASE = "wp_base"


class PageWriterType(StrEnum):
    POSTGRES_BASE = "postgres_base"


class TrafficBasedArticleRecommenderType(StrEnum):
    POPULARITY = "popularity"


class RecommendationWriterType(StrEnum):
    POSTGRES_BASE = "postgres_base"


class TaskType(StrEnum):
    UPDATE_PAGES = "update_pages"
    CREATE_TRAFFIC_BASED_RECOMMENDATIONS = "create_traffic_based_recommendations"


@dataclass
class EventFetcher:
    type: EventFetcherType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PageFetcher:
    type: PageFetcherType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class PageWriter:
    type: PageWriterType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class TrafficBasedArticleRecommender:
    type: TrafficBasedArticleRecommenderType
    params: dict[str, Any] = field(default_factory=dict)


@dataclass
class RecommendationWriter:
    type: RecommendationWriterType
    params: dict[str, Any] = field(default_factory=dict)


Component: TypeAlias = EventFetcher | PageFetcher | PageWriter | TrafficBasedArticleRecommender | RecommendationWriter


@dataclass
class Globals:
    site: str


@dataclass
class Components:
    event_fetchers: list[EventFetcher]
    page_fetchers: list[PageFetcher]
    page_writers: list[PageWriter]
    traffic_based_article_recommenders: list[TrafficBasedArticleRecommender]
    recommendation_writers: list[RecommendationWriter]


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
    _dict_components: dict[str, Component] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._dict_components = {
            component.type: component
            for component in self.components.event_fetchers
            + self.components.page_fetchers
            + self.components.page_writers
            + self.components.traffic_based_article_recommenders
        }

    def get_component(self, component_type: str) -> Component:
        component = self._dict_components.get(component_type)
        assert component is not None, f"Component of type {component_type} not found in config"
        return component


def create_config_object(config_dict: dict[str, Any]) -> Config:
    return dataclass_from_dict(data_class=Config, data=config_dict, config=DataclassFromDictConfig(cast=[StrEnum]))
