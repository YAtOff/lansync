from __future__ import annotations

from uuid import uuid4
from functools import lru_cache
from pathlib import Path

import peewee  # type: ignore

from lansync.database import database
from lansync.session import Session
from lansync.util.db_fields import JSONField
from lansync.util.misc import all_subclasses


class Device(peewee.Model):
    id = peewee.AutoField()
    device_id = peewee.CharField()

    class Meta:
        database = database

    @classmethod
    def default_device_id(cls) -> str:
        divice, _ = cls.get_or_create(device_id=uuid4().hex)
        return divice.device_id


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

    @classmethod
    def for_session(cls, session: Session) -> Namespace:
        return cls.by_name(session.namespace)


class RootFolder(peewee.Model):
    path = peewee.CharField(index=True)

    class Meta:
        database = database

    @classmethod  # type: ignore
    @lru_cache(maxsize=8)
    def by_path(cls, path):
        root_folder, _ = cls.get_or_create(path=path)
        return root_folder

    @classmethod
    def for_session(cls, session: Session) -> RootFolder:
        return cls.by_path(path=session.root_folder.fspath)


class StoredNode(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    root_folder = peewee.ForeignKeyField(RootFolder, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    path = peewee.CharField()
    checksum = peewee.CharField(null=True)
    parts = JSONField(null=True)
    local_modified_time = peewee.IntegerField()
    local_created_time = peewee.IntegerField()

    class Meta:
        database = database

    @property
    def local_path(self) -> Path:
        return Path(self.root_folder.path) / self.path


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
