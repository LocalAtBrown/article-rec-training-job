from dataclasses import dataclass


@dataclass
class Metrics:
    """
    Reporting metrics a recommendation writer must provide.
    """

    time_taken_to_write_recommendations: float
    num_recommendations_written: int
    num_embeddings_written: int
