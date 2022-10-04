from enum import Enum


class Strategy(Enum):
    """
    Simple Enum containing different recs-generating approaches, to be passed into Site object
    as a class property. Job will look at this to decide which scripts to run.

    TODO: Unify with db/model.py?
    """

    # Article-based collaborative filtering (as opposed to user-based, which is mentioned in db/model.py
    # but never used and generally have gone out of favor)
    COLLABORATIVE_FILTERING = "collaborative_filtering"

    # Semantic similarity
    SEMANTIC_SIMILARITY = "semantic_similarity"

    # Default popularity approach as API fallback. Will always be run.
    POPULARITY = "popularity"

    # Add new approaches here (which may use one or more models/model configs; for example,
    # an SS-CF hybrid requires both SS config and CF config)
    # e.g., HYBRID_SS_CF = "hybrid_ss_cf"
