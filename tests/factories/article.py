from datetime import datetime
from random import randint

from tests.factories.base import BaseFactory
from db.mappings.article import Article


class ArticleFactory(BaseFactory):
    mapping = Article

    @classmethod
    def make_defaults(cls):
        now = datetime.now()
        return {"external_id": str(randint(1000, 9000)), "published_at": str(now)}
