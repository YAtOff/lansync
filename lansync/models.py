from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import List, Tuple, Callable, Optional

import mong  # type: ignore
import peewee  # type: ignore

from lansync.database import database, atomic
from lansync.session import Session
from lansync import common
from lansync.util.db_fields import JSONField
from lansync.util.misc import all_subclasses
from lansync.util.file import read_chunk


class Device(peewee.Model):
    id = peewee.AutoField()
    device_id = peewee.CharField()

    class Meta:
        database = database

    @classmethod
    def default_device_id(cls) -> str:
        device = cls.select().first()
        if device is None:
            device_id = mong.get_random_name()
            while cls.select().where(cls.device_id == device_id).exists():
                device_id = mong.get_random_name()
            device = cls.create(device_id=device_id)
        return device.device_id


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
    local_modified_time = peewee.IntegerField()
    local_created_time = peewee.IntegerField()
    ready = peewee.BooleanField(default=False)
    size = peewee.IntegerField()
    signature = peewee.TextField()

    class Meta:
        database = database

    @property
    def local_path(self) -> Path:
        return Path(self.root_folder.path) / self.path

    @property
    def chunks(self) -> List[common.NodeChunk]:
        chunks = (
            NodeChunk.select()
            .join(Chunk, on=(NodeChunk.chunk == Chunk.id))
            .join(StoredNode, on=(NodeChunk.node == StoredNode.id))
            .where(NodeChunk.node == self)
        )
        return [
            common.NodeChunk(hash=c.chunk.hash, size=c.chunk.size, offset=c.offset) for c in chunks
        ]

    def sync_with_local(self, local_node):
        self.local_modified_time = local_node.modified_time
        self.local_created_time = local_node.created_time
        self.ready = True
        self.save()


class Chunk(peewee.Model):
    id = peewee.AutoField()
    hash = peewee.CharField(index=True)
    size = peewee.IntegerField()

    class Meta:
        database = database

    @classmethod
    def update_or_create(cls, hash: str, size: int) -> Chunk:
        chunk, _ = cls.get_or_create(hash=hash, defaults={"size": size})
        if chunk.size != size:
            chunk.size = size
            chunk.save()
        return chunk


class NodeChunk(peewee.Model):
    id = peewee.AutoField()
    node = peewee.ForeignKeyField(StoredNode, on_delete="CASCADE")
    chunk = peewee.ForeignKeyField(Chunk, on_delete="CASCADE")
    offset = peewee.IntegerField()

    class Meta:
        database = database

    @classmethod
    def update_or_create(cls, node: StoredNode, chunk: common.NodeChunk) -> NodeChunk:
        with atomic():
            chunk_db_instance = Chunk.update_or_create(chunk.hash, chunk.size)
            node_chunk, _ = cls.get_or_create(
                node=node, chunk=chunk_db_instance, defaults={"offset": chunk.offset}
            )
            if node_chunk.offset != chunk.offset:
                node_chunk.offset = chunk.offset
                node_chunk.save()
            return node_chunk

    @classmethod
    def find(
        cls, namespace: str, hash: str
    ) -> Optional[Tuple[common.NodeChunk, Callable[[], bytes]]]:
        try:
            namespace = Namespace.by_name(namespace)
            node_chunk = (
                NodeChunk.select()
                .join(Chunk, on=(NodeChunk.chunk == Chunk.id))
                .join(StoredNode, on=(NodeChunk.node == StoredNode.id))
                .where(StoredNode.namespace == namespace, Chunk.hash == hash)
                .first()
            )
            return (
                (
                    common.NodeChunk(
                        hash=node_chunk.chunk.hash,
                        size=node_chunk.chunk.size,
                        offset=node_chunk.offset,
                    ),
                    lambda: read_chunk(
                        node_chunk.node.local_path, node_chunk.offset, node_chunk.chunk.size
                    ),
                )
                if node_chunk
                else None
            )
        except peewee.DoesNotExist:
            return None


class RemoteNode(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    sequence_number = peewee.IntegerField(index=True)
    path = peewee.CharField()
    timestamp = peewee.CharField()
    checksum = peewee.CharField(null=True)
    chunks = JSONField(null=True)
    size = peewee.IntegerField()
    signature = peewee.TextField()

    class Meta:
        database = database

    def updated(self, stored: StoredNode) -> bool:
        return self.checksum != stored.checksum

    @classmethod
    def max_sequence_number(cls, namespace: Namespace) -> int:
        return (
            cls.select(peewee.fn.Max(cls.sequence_number))
            .where(cls.namespace == namespace)
            .scalar()
        )


class Market(peewee.Model):
    id = peewee.AutoField()
    namespace = peewee.ForeignKeyField(Namespace, on_delete="CASCADE")
    key = peewee.CharField(index=True)
    data = peewee.BlobField()

    class Meta:
        database = database

    @classmethod
    def find(cls, namespace: str, key: str) -> Optional[Market]:
        return (
            cls.select()
            .join(Namespace, on=(Namespace.id == cls.namespace))
            .where(Namespace.name == namespace, cls.key == key)
            .first()
        )


all_models = all_subclasses(peewee.Model)
