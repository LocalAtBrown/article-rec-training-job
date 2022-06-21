from peewee import TextField

from db.mappings.base import BaseMapping


class Path(BaseMapping):
    class Meta:
        table_name = "path"

    path = TextField(null=False, default="")
    site = TextField(null=False, default="")
    external_id = TextField(null=True, default="")
    exclude_reason = TextField(null=True, default="")
