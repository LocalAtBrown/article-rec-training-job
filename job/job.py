import logging

from db.models.model import Type
from job.helpers import create_model, create_article, create_rec


def rand_int():
    import random

    return random.randint(100000, 999999)


def run():
    logging.info("Running job...")

    # TODO this is just a test of our object mappings
    model_id = create_model(type=Type.ARTICLE.value)
    article_id = create_article(external_id=rand_int())

    rec_id = create_rec(
        external_id=str(rand_int()), model_id=model_id, article_id=article_id, score=0.000001
    )

    logging.info(f"Created rec with id {rec_id}")
