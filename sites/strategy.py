from enum import Enum


class Strategy(Enum):
    """
    Simple Enum containing different recs-generating approaches, to be passed into Site object
    as a class property. Job will look at this to decide which scripts to run.

    TODO: Unify with db/model.py?
    """

    COLLABORATIVE_FILTERING = "collaborative_filtering"
    SEMANTIC_SIMILARITY = "semantic_similarity"
    POPULARITY = "popularity"  # Default popularity approach as fallback. Will always be run.
    # Add new approaches here (which may use one or more models/model configs, e.g., SS, CF)
    # e.g., HYBRID_SS_CF = "hybrid_ss_cf"
