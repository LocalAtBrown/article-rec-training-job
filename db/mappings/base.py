from datetime import datetime, timezone, timedelta

from peewee import Model
from playhouse.shortcuts import model_to_dict
from playhouse.postgres_ext import DateTimeTZField as _DateTimeTZField

from lib.db import db


def tzaware_now() -> datetime:
    return datetime.now(timezone.utc)


class DateTimeTZField(_DateTimeTZField):
    def db_value(self, value):
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        else:
            return datetime.fromisoformat(value).astimezone(timezone.utc)

    def python_value(self, value):
        return value.astimezone(timezone.utc)


class BaseMapping(Model):
    created_at = DateTimeTZField(null=False, default=tzaware_now)
    updated_at = DateTimeTZField(null=False, default=tzaware_now)

    class Meta:
        database = db

    def to_dict(self):
        resource = model_to_dict(self)
        for key in resource.keys():
            if isinstance(resource[key], datetime):
                resource[key] = resource[key].isoformat()
        return resource
