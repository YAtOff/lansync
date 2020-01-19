from __future__ import annotations

import os
from dataclasses import dataclass, field
import logging
from pathlib import Path
import shutil
from uuid import uuid4
from typing import Set, Optional, NamedTuple, List, Dict

from lansync.database import atomic
from lansync.models import (
    RemoteNode, StoredNode, RootFolder, NodeChunk as NodeChunkModel,
    Namespace
)
from lansync.session import Session
from lansync.common import NodeChunk
from lansync.util.file import (
    hash_path,
    file_checksum,
    read_file_chunks,
    read_chunk,
    write_chunk,
    create_file_placeholder,
)
from lansync.util.misc import index_by


@dataclass
class LocalNode:
    root_folder: Path
    path: str
    key: str = field(init=False)
    modified_time: float
    created_time: float
    size: int
    _checksum: Optional[str]

    def __post_init__(self):
        self.key = hash_path(self.path)

    @classmethod
    def create(cls, local_path: Path, session: Session) -> LocalNode:
        root_folder = session.root_folder.path
        stat = local_path.stat()
        return LocalNode(
            root_folder=root_folder,
            path=local_path.relative_to(root_folder).as_posix(),
            modified_time=int(stat.st_mtime),
            created_time=int(stat.st_ctime),
            size=stat.st_size,
            _checksum=None,
        )

    @classmethod
    def create_placeholder(cls, local_path: Path, size: int, session: Session) -> LocalNode:
        if not local_path.exists():
            create_file_placeholder(local_path, size)
        return cls.create(local_path, session)

    def updated(self, stored: StoredNode) -> bool:
        return (
            self.modified_time != stored.local_modified_time
            or self.created_time != stored.local_created_time
        )

    @property
    def local_path(self) -> Path:
        return self.root_folder / self.path

    @property
    def local_fspath(self) -> str:
        return os.fspath(self.local_path)

    @property
    def checksum(self) -> str:
        if self._checksum is None:
            self._checksum = file_checksum(self.local_fspath) or ""
        return self._checksum

    @property
    def chunks(self) -> List[NodeChunk]:
        return [NodeChunk(*c) for c in read_file_chunks(self.local_fspath)]

    def read_chunk(self, chunk: NodeChunk) -> bytes:
        return read_chunk(self.local_path, chunk.offset, chunk.size)

    def write_chunk(self, chunk: NodeChunk, data: bytes) -> None:
        write_chunk(self.local_path, data, chunk.offset)
        NodeChunkModel.update_or_create(
            StoredNode.get(StoredNode.key == self.key),
            chunk
        )

    def transfer_chunk(self, src: Path, chunk: NodeChunk):
        data = read_chunk(src, chunk.offset, chunk.size)
        self.write_chunk(chunk, data)


class FullNode(NamedTuple):
    stored_node: StoredNode
    local_node: LocalNode
    all_chunks: Dict[str, List[NodeChunk]]
    needed_chunks: Set[str]
    available_chunk: Set[str]


def store_new_node(
    local_node: LocalNode, session: Session, stored_node: Optional[StoredNode]
) -> FullNode:
    chunks = local_node.chunks
    with atomic():
        new_node = StoredNode.create(
            namespace=Namespace.for_session(session),
            root_folder=RootFolder.for_session(session),
            key=str(uuid4()),
            path=local_node.path,
            checksum=local_node.checksum,
            size=local_node.size,
            local_modified_time=local_node.modified_time,
            local_created_time=local_node.created_time,
            ready=False,
        )

        for chunk in chunks:
            NodeChunkModel.update_or_create(new_node, chunk)

        if stored_node:
            stored_node.delete_instance()

        new_node.key = local_node.key
        new_node.ready = True
        new_node.save()

        all_chunks = index_by("hash")(chunks)
        return FullNode(
            new_node, local_node, all_chunks, set(), set(all_chunks.keys())
        )


def create_node_placeholder(remote_node: RemoteNode, session: Session) -> FullNode:
    with atomic():
        temp_path = str(uuid4())
        stored_node = StoredNode.create(
            namespace=remote_node.namespace,
            root_folder=RootFolder.for_session(session),
            key=temp_path,
            path=temp_path,
            checksum=remote_node.checksum,
            size=remote_node.size,
            local_modified_time=0,
            local_created_time=0,
            ready=False,
        )
        local_node = LocalNode.create_placeholder(
            stored_node.local_path, stored_node.size, session
        )

        all_chunks = index_by("hash")(NodeChunk(**c) for c in remote_node.chunks)
        needed_chunks: Set[str] = set()
        available_chunks: Set[str] = set()
        for chunk_hash, chunks in all_chunks.items():
            node_chunk_pair = NodeChunkModel.find(session.namespace, chunk_hash)
            if node_chunk_pair:
                available_chunk, read_chunk = node_chunk_pair
                logging.info(
                    "[CHUNK] found local chunk for node [%s]: [%r]", local_node.path, available_chunk
                )
                for chunk in chunks:
                    local_node.write_chunk(chunk, read_chunk())
                available_chunks.add(chunk_hash)
            else:
                needed_chunks.add(chunk_hash)

        StoredNode.delete().where(StoredNode.key == remote_node.key).execute()
        stored_node.key = remote_node.key
        stored_node.path = remote_node.path
        stored_node.save()

        shutil.move(local_node.local_path, stored_node.local_path)
        local_node = LocalNode.create(stored_node.local_path, session)

        return FullNode(stored_node, local_node, all_chunks, needed_chunks, available_chunks)
