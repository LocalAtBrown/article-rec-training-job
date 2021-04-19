import datetime

from peewee import TextField, IntegerField

from db.mappings.base import BaseMapping, DateTimeTZField


class Article(BaseMapping):
    class Meta:
        table_name = "article"

    external_id = IntegerField(null=False)
    title = TextField(null=False, default="")
    path = TextField(null=False, default="")
    published_at = DateTimeTZField(null=True)
