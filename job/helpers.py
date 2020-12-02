import requests as req

from db.models.base import BaseModel
from db.models.model import Model
from db.models.article import Article
from db.models.recommendation import Rec


def create_model(**params: dict) -> int:
    return create(Model, **params)


def create_article(**params: dict) -> int:
    return create(Article, **params)


def create_rec(**params: dict) -> int:
    return create(Rec, **params)


def create(model_class: BaseModel, **params: dict) -> int:
    resource = model_class(**params)
    resource.save()
    return resource.id


def safe_get(url):
    TIMEOUT_SECONDS = 60
    page = req.get(url, timeout=TIMEOUT_SECONDS)
    return page
