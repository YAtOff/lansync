import json

import peewee  # tpye: ignore

from lansync.database import database
from lansync.util.misc import all_subclasses


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class Namespace(peewee.Model):
    name = peewee.CharField(index=True)

    class Meta:
        database = database


class Node(peewee.Model):
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    path = peewee.CharField()
    checksum = peewee.CharField()
    timestamp = peewee.CharField()
    parts = JSONField()

    class Meta:
        database = database


all_models = all_subclasses(peewee.Model)
