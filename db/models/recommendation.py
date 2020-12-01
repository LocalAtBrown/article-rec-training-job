from peewee import TextField, DecimalField, ForeignKeyField

from db.models.base import BaseModel
from db.models.model import Model
from db.models.article import Article


class Rec(BaseModel):
    class Meta:
        db_table = "recommendation"

    external_id = TextField(null=False)
    model = ForeignKeyField(Model, null=False)
    article = ForeignKeyField(Article, null=False)
    score = DecimalField(max_digits=7, decimal_places=6)
