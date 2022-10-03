from dataclasses import dataclass
from typing import List, Optional, Set, TypedDict

# Custom types
# TypedDict (as opposed to dataclass) doesn't require changing existing CF code in the job directory
# to work with it, but open to stricter suggestions.


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
    Default popularity model configs.
    """

    popularity_window: int


@dataclass
class SiteConfig:
    """
    Site config object for different model configs
    """

    # Popularity model is fallback and therefore its config is not optional
    popularity: ConfigPop
    collaborative_filtering: Optional[ConfigCF] = None
    semantic_similarity: Optional[ConfigSS] = None
