import enum

from sqlalchemy import Column
from sqlalchemy.types import Integer, Enum, DateTime
from sqlalchemy.sql.functions import now

from db.base import Base


class Type(enum.Enum):
    # in the future, we imagine supporting 'user' and 'cluster' types
    article = 'article'


class Status(enum.Enum):
    pending = 'pending'
    current = 'current'
    stale = 'stale'
    failed = 'failed'


class Model(Base):
    __tablename__ = 'model'

    id         = Column(Integer,
                        primary_key=True)
    type       = Column(Enum(Type))
    status     = Column(Enum(Status),
                        default=Status.pending.name)
    created_at = Column(DateTime,
                        default=now())
    updated_at = Column(DateTime,
                        default=now(),
                        onupdate=now())
