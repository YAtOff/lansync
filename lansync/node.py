from __future__ import annotations

import logging
import os
import shutil
from base64 import b64encode
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, NamedTuple, Optional, Set, Union
from uuid import uuid4

from dynaconf import settings  # type: ignore
from typing_extensions import Literal

import librsync  # type: ignore

from lansync.chunk import NodeChunk, calc_initial_chunks, calc_new_chunks
from lansync.database import atomic
from lansync.models import Namespace
from lansync.models import NodeChunk as NodeChunkModel
from lansync.models import RootFolder, StoredNode
from lansync.session import Session
from lansync.serializers import RemoteNodeSerializer
from lansync.util.file import (
    create_file_placeholder, create_temp_file,
    file_checksum, hash_path,
    read_chunk, write_chunk
)
from lansync.util.misc import index_by
from lansync.util.timeutil import now_as_iso


@dataclass
class RemoteNode:
    namespace: str
    key: str
    path: str
    timestamp: str
    checksum: str
    size: int
    chunks: List[NodeChunk]
    signature: str

    @classmethod
    def create(
        cls, namespace: str, local_node: LocalNode,
        chunks: List[NodeChunk], signature: str
    ) -> RemoteNode:
        return cls(
            namespace=namespace,
            key=local_node.key,
            path=local_node.path,
            timestamp=now_as_iso(),
            checksum=local_node.checksum,
            size=local_node.size,
            chunks=chunks,
            signature=signature
        )

    @classmethod
    def load(cls, data: Dict) -> RemoteNode:
        data = RemoteNodeSerializer().load(data)
        chunks = [NodeChunk(**c) for c in data.pop("chunks")]
        return cls(chunks=chunks, **data)

    def dump(self) -> Dict:
        return RemoteNodeSerializer().dump(asdict(self))


@dataclass
class LocalNode:
    root_folder: Path
    path: str
    key: str = field(init=False)
    modified_time: float
    created_time: float
    size: int
    _checksum: Optional[str]
    _signature: Optional[bytes] = None

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

    def calc_signature(self, format=Union[Literal["binary"], Literal["base64"]]) -> Union[str, bytes]:
        if self._signature is None:
            with create_temp_file() as f:
                librsync.signature_from_paths(
                    self.local_fspath, f, block_len=settings.CHUNK_SIZE
                )
                self._signature = Path(f).read_bytes()

        if format == "binary":
            return self._signature
        elif format == "base64":
            return b64encode(self._signature)
        else:
            raise ValueError("Invalid format", format)

    def calc_chunks(self, signature: Optional[str]) -> List[NodeChunk]:
        if signature is None:
            return calc_initial_chunks(self.local_fspath)
        else:
            return calc_new_chunks(self.local_fspath, signature)


class FullNode(NamedTuple):
    stored_node: StoredNode
    local_node: LocalNode
    remote_node: RemoteNode
    all_chunks: List[NodeChunk]
    chunk_index: Dict[str, List[NodeChunk]]
    needed_chunks: Set[str]
    available_chunks: Set[str]


def store_new_node(
    local_node: LocalNode, session: Session, stored_node: Optional[StoredNode]
) -> FullNode:
    if stored_node is not None:
        signature = stored_node.signature
        chunks = local_node.calc_chunks(signature=signature)
    else:
        signature = local_node.calc_signature(format="base64")
        chunks = local_node.calc_chunks(None)

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
            signature=signature
        )

        for chunk in chunks:
            NodeChunkModel.update_or_create(new_node, chunk)

        if stored_node:
            stored_node.delete_instance()

        new_node.key = local_node.key
        new_node.ready = True
        new_node.save()

        chunk_index = index_by("hash")(chunks)

        remote_node = RemoteNode.create(session.namespace, local_node, chunks, signature)
        return FullNode(
            new_node, local_node, remote_node,
            chunks, chunk_index, set(), set(chunk_index.keys())
        )


def create_node_placeholder(remote_node: RemoteNode, session: Session) -> FullNode:
    with atomic():
        temp_path = str(uuid4())
        stored_node = StoredNode.create(
            namespace=Namespace.by_name(remote_node.namespace),
            root_folder=RootFolder.for_session(session),
            key=hash_path(temp_path),
            path=temp_path,
            checksum=remote_node.checksum,
            size=remote_node.size,
            local_modified_time=0,
            local_created_time=0,
            ready=False,
            signature=remote_node.signature
        )
        local_node = LocalNode.create_placeholder(
            stored_node.local_path, stored_node.size, session
        )

        all_chunks = remote_node.chunks
        chunk_index = index_by("hash")(all_chunks)
        needed_chunks: Set[str] = set()
        available_chunks: Set[str] = set()
        for chunk_hash, chunks in chunk_index.items():
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

        return FullNode(
            stored_node, local_node, remote_node,
            all_chunks, chunk_index, needed_chunks, available_chunks
        )
