from sqlalchemy import Column
from sqlalchemy.types import Integer, String, DateTime
from sqlalchemy.sql.functions import now

from db.base import Base


class Article(Base):
    __tablename__ = 'article'

    id           = Column(Integer,
                          primary_key=True)
    external_id  = Column(Integer)
    title        = Column(String,
                          default='')
    published_at = Column(DateTime)
    created_at   = Column(DateTime,
                          default=now())
    updated_at   = Column(DateTime,
                          default=now(),
                          onupdate=now())
