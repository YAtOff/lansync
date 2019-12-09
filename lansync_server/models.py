import enum
import json
from uuid import uuid4

import peewee  # tpye: ignore

from lansync.database import database
from lansync.util.misc import all_subclasses


class NodeOperation(str, enum.Enum):
    CREATE = "create"
    DELETE = "delete"

    @classmethod
    def choices(cls):
        return [
            (cls.CREATE, "Create"),
            (cls.DELETE, "Delete"),
        ]

    def __str__(self):
        return self.value


class JSONField(peewee.TextField):
    def db_value(self, value):
        return json.dumps(value)

    def python_value(self, value):
        if value is not None:
            return json.loads(value)


class Namespace(peewee.Model):
    id = peewee.AutoField()
    name = peewee.CharField(index=True)

    class Meta:
        database = database


class NodeEvent(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    operation = peewee.CharField(choices=NodeOperation.choices())
    sequence_number = peewee.IntegerField(index=True)
    path = peewee.CharField()
    timestamp = peewee.CharField()
    checksum = peewee.CharField(null=True)
    parts = JSONField(null=True)

    class Meta:
        database = database


class Sequence(peewee.Model):
    key = peewee.CharField(unique=True)
    version = peewee.CharField()
    value = peewee.IntegerField(default=0)

    class Meta:
        database = database

    @classmethod
    def increment(cls, key: str) -> int:
        seq, created = cls.get_or_create(
            key=key, defaults={"version": uuid4().hex}
        )
        if created:
            return seq.value

        q = cls.update(value=cls.value + 1)\
            .where(cls.key == key, cls.version == seq.version)
        if q.execute() == 1:
            return seq.value + 1
        else:
            return cls.increment(key)


all_models = all_subclasses(peewee.Model)
