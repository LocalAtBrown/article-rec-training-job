import logging

from db.models.model import Model, Type
from db.models.article import Article
from db.models.recommendation import Rec


def rand_int():
    import random

    return random.randint(100000, 999999)


def run():
    logging.info("Running job...")

    # TODO this is just a test of our object mappings
    # we'll need a cleaner way to handle sessions
    model = Model(type=Type.ARTICLE.value)
    article = Article(external_id=rand_int())
    model.save()
    article.save()

    rec = Rec(external_id=str(rand_int()), model_id=model.id, article_id=article.id, score=0.000001)
    rec.save()

    logging.info(f"Created rec with id {rec.id}")
