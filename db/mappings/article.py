import datetime

from peewee import TextField, IntegerField

from db.mappings.base import BaseMapping, DateTimeTZField


class Article(BaseMapping):
    class Meta:
        table_name = "article"

    external_id = TextField(null=False, default="")
    title = TextField(null=False, default="")
    path = TextField(null=False, default="")
    site = TextField(null=False)
    published_at = DateTimeTZField(null=True)
