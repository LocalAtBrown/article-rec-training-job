from dataclasses import dataclass

from article_rec_db.models import Article, Page


@dataclass
class Output:
    pages: list[Page]
    articles: list[Article]
