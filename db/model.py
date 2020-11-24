import enum

from sqlalchemy import Column
from sqlalchemy.types import Integer, Enum, DateTime
from sqlalchemy.sql.functions import now

from db.base import Base


class Type(enum.Enum):
    # in the future, we imagine supporting 'user' and 'cluster' types
    ARTICLE = 'article'


class Status(enum.Enum):
    PENDING = 'pending'
    CURRENT = 'current'
    STALE = 'stale'
    FAILED = 'failed'


class Model(Base):
    __tablename__ = 'model'

    id = Column(Integer, primary_key=True)
    type       = Column(Enum(Type))
    status     = Column(Enum(Status),
                        default=Status.PENDING.value)
    created_at = Column(DateTime,
                        default=now())
    updated_at = Column(DateTime,
                        default=now(),
                        onupdate=now())
