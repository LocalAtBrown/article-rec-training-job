import logging
from typing import Iterable, List, Type

from peewee import Expression

from db.mappings.article import Article
from db.mappings.base import BaseMapping, db_proxy, tzaware_now
from db.mappings.model import Model, ModelType, Status
from db.mappings.recommendation import Rec
from sites.site import Site


def refresh_db(func):
    """
    Simple decorator to re-establish the database connection
    because it occasionally times out
    """

    def wrapper(*args, **kwargs):
        db_proxy.close()
        db_proxy.connect()
        return func(*args, **kwargs)

    return wrapper


def create_model(**params: dict) -> int:
    return create_resource(Model, **params)


def create_article(**params: dict) -> int:
    return create_resource(Article, **params)


def create_rec(**params: dict) -> int:
    return create_resource(Rec, **params)


def create_resource(mapping_class: Type[BaseMapping], **params: dict) -> int:
    resource = mapping_class(**params)
    resource.save()
    return resource.id


def get_resource(mapping_class: BaseMapping, _id: int) -> dict:
    instance = mapping_class.get(mapping_class.id == _id)
    return instance.to_dict()


def get_articles_by_external_ids(site: Site, external_ids: Iterable[str]) -> List[dict]:
    res = Article.select().where((Article.site == site.name) & Article.external_id.in_(list(external_ids)))
    if res:
        return [r for r in res]
    else:
        return []


def get_existing_external_ids(site: Site, external_ids: Iterable[str]) -> Iterable[str]:
    """
    Query the db with a list of external IDs and retrieve a list of the valid external IDs in the input
    """
    return [a.external_id for a in get_articles_by_external_ids(site, external_ids)]


def update_article(article_id, **params) -> None:
    _update_resources(Article, Article.id == article_id, **params)


def update_model(model_id, **params) -> None:
    _update_resources(Model, Model.id == model_id, **params)


def _update_resources(mapping_class: BaseMapping, conditions: Expression, **params: dict) -> None:
    params["updated_at"] = tzaware_now()
    q = mapping_class.update(**params).where(conditions)
    q.execute()


def delete_articles(external_ids: List[str]) -> None:
    _delete_resources(Article, Article.external_id.in_(external_ids))


def delete_models(model_ids: List[int]) -> None:
    logging.info(f"Deleting {len(model_ids)} models: {model_ids}")
    _delete_resources(Model, Model.id.in_(model_ids))


def _delete_resources(mapping_class: BaseMapping, conditions: Expression) -> None:
    dq = mapping_class.delete().where(conditions)
    dq.execute()


def set_stale_model(model_type: ModelType, model_site: str) -> None:
    current_model_query = (Model.type == model_type) & (Model.status == Status.CURRENT.value) & (Model.site == model_site)
    _update_resources(Model, current_model_query, status=Status.STALE.value)


def get_stale_model_ids(model_type: ModelType, model_site: str) -> List[int]:
    stale_model_query = (Model.type == model_type) & (Model.status == Status.STALE.value) & (Model.site == model_site)
    query = Model.select(Model.id).where(stale_model_query)
    stale_model_ids = [res.id for res in query]
    logging.info(f"Found {len(stale_model_ids)} stale '{model_type}' models")
    return stale_model_ids


# If an exception occurs, the current transaction/savepoint will be rolled back.
# Otherwise the statements will be committed at the end.
@db_proxy.atomic()
def set_current_model(model_id: int, model_type: ModelType, model_site: str) -> None:
    MAX_DELETES = 2
    # get stale models
    stale_model_ids = get_stale_model_ids(model_type, model_site)
    # set current model as stale
    set_stale_model(model_type, model_site)
    # set pending model as current
    update_model(model_id, status=Status.CURRENT.value)
    # delete stale models
    delete_models(stale_model_ids[:MAX_DELETES])

    logging.info(f"Successfully updated model id {model_id} as current '{model_type}' model'")


def get_articles_by_path(site: str, paths: List[str]) -> List[Article]:
    query = Article.select().where(Article.site == site)
    logging.info(f"Found {query.count()} articles by path")
    return query.where(Article.path.in_(paths))
