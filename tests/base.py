import functools
from unittest import TestCase

from peewee import SqliteDatabase

from db.mappings import database
from db.mappings.model import Model
from db.mappings.article import Article
from db.mappings.recommendation import Rec
from db.mappings.path import Path


MAPPINGS = (Model, Article, Rec, Path)


def recreate_tables(db):
    db.drop_tables(MAPPINGS)
    db.create_tables(MAPPINGS)


class BaseTest(TestCase):
    def setUp(self):
        assert isinstance(database, SqliteDatabase), "database must be sqlite for tests"
        recreate_tables(database)
        super().setUp()
