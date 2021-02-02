from datetime import datetime

from db.mappings.base import BaseMapping
from db.mappings.model import Model, Type, Status
from db.mappings.article import Article
from db.mappings.recommendation import Rec
from lib.db import db


def create_model(**params: dict) -> int:
    return _create_resource(Model, **params)


def create_article(**params: dict) -> int:
    return _create_resource(Article, **params)


def create_rec(**params: dict) -> int:
    return _create_resource(Rec, **params)


def _create_resource(mapping_class: BaseMapping, **params: dict) -> int:
    resource = mapping_class(**params)
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


def update_model(model_id, **params) -> None:
    _update_resource(Model, model_id, **params)


def _update_resource(mapping_class: BaseMapping, resource_id: int, **params: dict) -> None:
    params["updated_at"] = datetime.now()
    q = mapping_class.update(**params).where(mapping_class.id == resource_id)
    q.execute()


# If an exception occurs, the current transaction/savepoint will be rolled back.
# Otherwise the statements will be committed at the end.
@db.atomic()
def set_current_model(current_model_id: int, model_type: Type) -> None:
    update_stale_models = Model.update(status=Status.STALE.value).where(
        (Model.type == model_type) & (Model.status == Status.CURRENT.value)
    )
    update_stale_models.execute()
    update_model(current_model_id, status=Status.CURRENT.value)
