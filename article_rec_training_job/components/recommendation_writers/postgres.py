from dataclasses import dataclass, field
from typing import final

from article_rec_db.models import Recommender
from loguru import logger
from sqlalchemy.orm import Session, sessionmaker

from article_rec_training_job.shared.helpers.time import get_elapsed_time
from article_rec_training_job.shared.types.recommendation_writers import Metrics


@dataclass
class BaseWriter:
    """
    Writes recommendations along with embeddings (if any) to the database.
    These are wrapped in a Recommender object.
    """

    # SQLAlchemy session factory
    sa_session_factory: sessionmaker[Session]

    num_recommendations_written: int = field(init=False, repr=False)
    num_embeddings_written: int = field(init=False, repr=False)

    @get_elapsed_time
    def _write(self, recommender: Recommender) -> None:
        logger.info("Writing recommendations to database...")
        with self.sa_session_factory() as session:
            session.add(recommender)
            session.commit()

            self.num_recommendations_written = len(recommender.recommendations)
            self.num_embeddings_written = len(recommender.embeddings)

    @final
    def write(self, recommender: Recommender) -> Metrics:
        """
        Writes recommendations to the database.
        """
        time_taken_to_write_recommendations, _ = self._write(recommender)
        return Metrics(
            time_taken_to_write_recommendations=time_taken_to_write_recommendations,
            num_recommendations_written=self.num_recommendations_written,
            num_embeddings_written=self.num_embeddings_written,
        )
