import datetime

from peewee import TextField, DateTimeField, IntegerField

from db.models.base import BaseModel


class Article(BaseModel):
    class Meta:
        db_table = "article"

    external_id = IntegerField(null=False)
    title = TextField(null=False, default="")
    published_at = DateTimeField(null=True)
