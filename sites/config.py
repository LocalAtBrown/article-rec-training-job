from dataclasses import dataclass
from typing import List, Optional, Set, TypedDict

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


@dataclass
class ConfigCF:
    """
    Collborative-filtering site configs.
    """

    snowplow_fields: Set[str]
    scrape_config: ScrapeConfig
    training_params: TrainParamsCF
    # this is a number of years; will grab dwell time data for any article within the past X years
    max_article_age: int


@dataclass
class ConfigSS:
    """
    TODO: Semantic-similarity site configs.
    """

    pass


@dataclass
class ConfigPop:
    """
    Default popularity-model site configs.
    """

    # this is a number of days; will only recommend articles within the past X days
    popularity_window: int


@dataclass
class SiteConfig:
    """
    Site config object for different model configs

    In order to add a new model (not a new approach/strategy, which may or may not require adding a new model)
    to the site:
    1. Create a Config<ModelName> dataclass with appropriate config variables and variable types
    2. Add created config dataclass to this class as a property
    """

    # Popularity model is fallback and therefore its config is not optional
    popularity: ConfigPop
    collaborative_filtering: Optional[ConfigCF] = None
    semantic_similarity: Optional[ConfigSS] = None
