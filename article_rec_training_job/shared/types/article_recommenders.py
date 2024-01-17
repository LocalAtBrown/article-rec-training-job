from dataclasses import dataclass
from enum import StrEnum


class Strategy(StrEnum):
    """
    Strategy for recommending articles. Include all strategies
    (across all recommender types) here.
    """

    POPULARITY = "popularity"


@dataclass(frozen=True)
class Metrics:
    """
    Reporting metrics an article recommender must provide.
    """

    # raise NotImplementedError
    pass
