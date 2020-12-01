import datetime

from peewee import Model, DateTimeField

from lib.db import db


class Base(Model):
    created_at = DateTimeField(null=False, default=datetime.datetime.now)
    updated_at = DateTimeField(null=False, default=datetime.datetime.now)

    class Meta:
        database = db
