import logging

from peewee import Expression
from typing import List, Iterable

from db.mappings.base import BaseMapping, tzaware_now
from db.mappings.model import Model, Type, Status
from db.mappings.article import Article
from db.mappings.recommendation import Rec
from db.mappings.base import db_proxy

from sites.site import Site


def create_model(**params: dict) -> int:
    return create_resource(Model, **params)


def create_article(**params: dict) -> int:
    return create_resource(Article, **params)


def create_rec(**params: dict) -> int:
    return create_resource(Rec, **params)


def create_resource(mapping_class: BaseMapping, **params: dict) -> int:
    resource = mapping_class(**params)
    resource.save()
    return resource.id


def get_resource(mapping_class: BaseMapping, _id: int) -> dict:
    instance = mapping_class.get(mapping_class.id == _id)
    return instance.to_dict()


def get_articles_by_external_ids(site: Site, external_ids: List[str]) -> List[dict]:
    res = Article.select().where(
        (Article.site == site.name) & Article.external_id.in_(external_ids)
    )
    if res:
        return [r for r in res]
    else:
        return []


def update_article(article_id, **params) -> None:
    _update_resources(Article, Article.id == article_id, **params)


def update_model(model_id, **params) -> None:
    _update_resources(Model, Model.id == model_id, **params)


def _update_resources(
    mapping_class: BaseMapping, conditions: Expression, **params: dict
) -> None:
    params["updated_at"] = tzaware_now()
    q = mapping_class.update(**params).where(conditions)
    q.execute()


def delete_articles(external_ids: List[str]) -> None:
    _delete_resources(Article, Article.external_id.in_(external_ids))


def delete_models(model_ids: List[int]) -> None:
    _delete_resources(Model, Model.id.in_(model_ids))


def _delete_resources(mapping_class: BaseMapping, conditions: Expression) -> None:
    dq = mapping_class.delete().where(conditions)
    dq.execute()


# If an exception occurs, the current transaction/savepoint will be rolled back.
# Otherwise the statements will be committed at the end.
@db_proxy.atomic()
def set_current_model(model_id: int, model_type: Type, model_site: str) -> None:
    current_model_query = (
        (Model.type == model_type)
        & (Model.status == Status.CURRENT.value)
        & (Model.site == model_site)
    )
    _update_resources(Model, current_model_query, status=Status.STALE.value)
    update_model(model_id, status=Status.CURRENT.value)
    logging.info(
        f"Successfully updated model id {model_id} as current '{model_type}' model for '{model_site}'"
    )
