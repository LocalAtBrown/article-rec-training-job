from sqlalchemy import Column
from sqlalchemy.types import Integer, String, Numeric, DateTime
from sqlalchemy import ForeignKey
from sqlalchemy.sql.functions import now

from db.base import Base


class Rec(Base):
    __tablename__ = "recommendation"

    id = Column(Integer, primary_key=True)
    external_id = Column(String)
    model_id = Column(Integer, ForeignKey("model.id"))
    article_id = Column(Integer, ForeignKey("article.id"))
    score = Column(Numeric(precision=7, scale=6))
    created_at = Column(DateTime, default=now())
    updated_at = Column(DateTime, default=now(), onupdate=now())
