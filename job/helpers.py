from datetime import datetime, timezone, timedelta
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


def should_refresh(publish_ts: str) -> bool:
    # refresh metadata without a published time recorded yet
    if not publish_ts:
        return True

    # refresh metadata for articles published within the last day
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    if datetime.fromisoformat(publish_ts) > yesterday:
        return True

    return False


def find_or_create_article(external_id: int, path: str) -> int:
    article = get_article_by_external_id(external_id)
    if article:
        if should_refresh(article["published_at"]):
            metadata = scrape_article_metadata(path)
            update_article(article["id"], **metadata)
        return article["id"]

    metadata = scrape_article_metadata(path)
    article_data = {**metadata, "external_id": external_id}
    article_id = create_article(**article_data)

    return article_id


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

    for path in paths[10:15]:
        external_id = extract_external_id(path)
        if external_id:
            article_id = find_or_create_article(external_id, path)
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
