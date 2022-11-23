from dataclasses import dataclass
from typing import List, Set, TypedDict

# Custom types
# TypedDict (as opposed to dataclass) doesn't require changing existing CF code to work with it
# (especially when CF train_model.py requires train_params to be a dict in order to call dict.update)


class ScrapeConfig(TypedDict):
    """
    Scraping config.
    """

    concurrent_requests: int
    requests_per_second: int


class TrainParamsCF(TypedDict):
    """
    Training params for collaborative filtering.
    """

    hl: int
    embedding_dim: int
    epochs: int
    tune: bool
    tune_params: List[str]
    tune_ranges: List[List[int]]
    model: str
    loss: str


# Config dataclasses


class StrategyConfig:
    pass


@dataclass
class ConfigCF(StrategyConfig):
    """
    Collaborative-filtering site configs.
    """

    snowplow_fields: Set[str]
    scrape_config: ScrapeConfig
    training_params: TrainParamsCF
    # this is a number of years; will grab dwell time data for any article within the past X years
    max_article_age: int


@dataclass
class ConfigSS(StrategyConfig):
    """
    TODO: Semantic-similarity site configs.
    """

    pass


@dataclass
class ConfigPop(StrategyConfig):
    """
    Default popularity-model site configs.
    """

    # this is a number of days; will only recommend articles within the past X days
    popularity_window: int
