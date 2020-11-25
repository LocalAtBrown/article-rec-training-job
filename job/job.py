import logging

from lib.db import Session
from db.model import Model, Type
from db.article import Article
from db.recommendation import Rec


def rand_int():
    import random

    return random.randint(100000, 999999)


def run():
    logging.info("Running job...")

    # TODO this is just a test of our object mappings
    # we'll need a cleaner way to handle sessions
    session = Session()
    model = Model(type=Type.article.name)
    article = Article(external_id=rand_int())
    session.add(model)
    session.add(article)
    session.commit()

    session = Session()
    rec = Rec(external_id=str(rand_int()), model_id=model.id, article_id=article.id, score=0.000001)
    session.add(rec)
    session.commit()
