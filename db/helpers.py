from datetime import datetime

from db.models.base import BaseModel
from db.models.model import Model
from db.models.article import Article
from db.models.recommendation import Rec


def create_model(**params: dict) -> int:
    return _create_resource(Model, **params)


def create_article(**params: dict) -> int:
    return _create_resource(Article, **params)


def create_rec(**params: dict) -> int:
    return _create_resource(Rec, **params)


def _create_resource(model_class: BaseModel, **params: dict) -> int:
    resource = model_class(**params)
    resource.save()
    return resource.id


def get_article_by_external_id(external_id: int) -> dict:
    res = Article.select().where(Article.external_id == external_id)
    if res:
        return res[0].to_dict()
    else:
        return None


def update_article(article_id, **params) -> None:
    _update_resource(Article, article_id, **params)


def _update_resource(model_class: BaseModel, resource_id: int, **params: dict) -> None:
    params["updated_at"] = datetime.now()
    q = model_class.update(**params).where(model_class.id == resource_id)
    q.execute()
