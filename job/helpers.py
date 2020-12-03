import re
import requests as req

from bs4 import BeautifulSoup

from db.models.base import BaseModel
from db.models.model import Model
from db.models.article import Article
from db.models.recommendation import Rec


PATH_PATTERN = "\/article\/(\d+)\/\S+"
PATH_PROG = re.compile(PATH_PATTERN)


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


def find_or_create_article(external_id: int, path: str) -> dict:
    res = Article.select().where(Article.external_id == external_id)
    if res:
        # TODO if article was published in the last 24 hrs, update metadata
        return res[0].to_dict()

    metadata_dict = scrape_article_metadata(path)
    article_id = create_article(external_id=external_id, **metadata_dict)
    res = Article.select().where(Article.id == article_id)
    article = res[0].to_dict()

    return article


def safe_get(url: str) -> str:
    TIMEOUT_SECONDS = 60
    page = req.get(url, timeout=TIMEOUT_SECONDS)
    return page


def extract_external_id(path: str) -> int:
    result = PATH_PROG.match(path)
    if result:
        return int(result.groups()[0])
    else:
        return None


def find_or_create_articles(paths: list) -> dict:
    article_dict = {}

    for path in paths[:10]:
        external_id = extract_external_id(path)
        if external_id:
            article = find_or_create_article(external_id, path)
            article_id = article["id"]
            article_dict[external_id] = article_id

    return article_dict


def scrape_article_metadata(path: str) -> dict:
    DOMAIN = "https://washingtoncitypaper.com"
    url = f"{DOMAIN}{path}"
    page = safe_get(url)
    soup = BeautifulSoup(page.text, features="html.parser")
    metadata = {}

    meta_tags = [
        ("title", "og:title"),
        ("published_at", "article:published_time"),
    ]

    for name, prop in meta_tags:
        tag = soup.find("meta", property=prop)
        metadata[name] = tag.get("content")

    return metadata
