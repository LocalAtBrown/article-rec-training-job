from db.mappings.model import Model, ModelType, Status
from tests.factories.base import BaseFactory


class ModelFactory(BaseFactory):
    mapping = Model

    @classmethod
    def make_defaults(cls):
        return {
            "type": ModelType.ARTICLE.value,
            "status": Status.CURRENT.value,
        }
