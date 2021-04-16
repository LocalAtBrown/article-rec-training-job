import logging

from datetime import datetime
from peewee import Expression
from typing import List

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


def get_articles_by_external_ids(external_ids: List[int]) -> List[dict]:
    res = Article.select().where(Article.external_id.in_(external_ids))
    if res:
        return [r.to_dict() for r in res]
    else:
        return []


def update_article(article_id, **params) -> None:
    _update_resources(Article, Article.id == article_id, **params)


def update_model(model_id, **params) -> None:
    _update_resources(Model, Model.id == model_id, **params)


def _update_resources(
    mapping_class: BaseMapping, conditions: Expression, **params: dict
) -> None:
    params["updated_at"] = datetime.now()
    q = mapping_class.update(**params).where(conditions)
    q.execute()


# If an exception occurs, the current transaction/savepoint will be rolled back.
# Otherwise the statements will be committed at the end.
@db.atomic()
def set_current_model(current_model_id: int, model_type: Type) -> None:
    current_model_query = (Model.type == model_type) & (
        Model.status == Status.CURRENT.value
    )
    _update_resources(Model, current_model_query, status=Status.STALE.value)
    update_model(current_model_id, status=Status.CURRENT.value)
    logging.info(f"Successfully updated current {model_type} model: {current_model_id}")
