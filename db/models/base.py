import datetime

from peewee import Model, DateTimeField
from playhouse.shortcuts import model_to_dict

from lib.db import db


class BaseModel(Model):
    created_at = DateTimeField(null=False, default=datetime.datetime.now)
    updated_at = DateTimeField(null=False, default=datetime.datetime.now)

    class Meta:
        database = db

    def to_dict(self):
        return model_to_dict(self)
