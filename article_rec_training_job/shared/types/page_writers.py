from dataclasses import dataclass


@dataclass(frozen=True)
class Metrics:
    """
    Reporting metrics a page writer must provide.
    """

    num_new_pages_written: int
    num_pages_ignored: int
    num_articles_upserted: int
    num_articles_ignored: int
    time_taken_to_write_pages: float
