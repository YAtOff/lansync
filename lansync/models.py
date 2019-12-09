from __future__ import annotations

from functools import lru_cache

import peewee  # type: ignore

from lansync.database import database
from lansync.util.db_fields import JSONField
from lansync.util.misc import all_subclasses


class Namespace(peewee.Model):
    id = peewee.AutoField()
    name = peewee.CharField(index=True)

    class Meta:
        database = database

    @classmethod  # type: ignore
    @lru_cache(maxsize=8)
    def by_name(cls, name):
        namespace, _ = cls.get_or_create(name=name)
        return namespace


class StoredNode(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    path = peewee.CharField()
    checksum = peewee.CharField(null=True)
    parts = JSONField(null=True)
    local_modified_time = peewee.IntegerField()
    local_created_time = peewee.IntegerField()

    class Meta:
        database = database


class RemoteNode(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    sequence_number = peewee.IntegerField(index=True)
    path = peewee.CharField()
    timestamp = peewee.CharField()
    checksum = peewee.CharField(null=True)
    parts = JSONField(null=True)

    class Meta:
        database = database

    def updated(self, stored: StoredNode) -> bool:
        return self.checksum != stored.checksum

    @classmethod
    def max_sequence_number(cls, namespace: Namespace) -> int:
        return cls.select(peewee.fn.Max(cls.sequence_number))\
            .where(cls.namespace == namespace)\
            .scalar()


all_models = all_subclasses(peewee.Model)
