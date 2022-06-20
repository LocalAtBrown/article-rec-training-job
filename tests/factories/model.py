from db.mappings.model import Model, Status, Type
from tests.factories.base import BaseFactory


class ModelFactory(BaseFactory):
    mapping = Model

    @classmethod
    def make_defaults(cls):
        return {
            "type": Type.ARTICLE.value,
            "status": Status.CURRENT.value,
        }
