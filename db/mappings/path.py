from peewee import TextField, IntegerField

from db.mappings.base import BaseMapping, DateTimeTZField


class Path(BaseMapping):
    class Meta:
        table_name = "paths"

    path = TextField(null=False, default="")
    site = TextField(null=False, default="")
    external_id = TextField()
    exclude_reason = TextField()
