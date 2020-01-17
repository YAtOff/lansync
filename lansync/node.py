from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from lansync.database import atomic
from lansync.session import Session
from lansync.models import StoredNode, Namespace, RootFolder, NodeChunk as NodeChunkModel
from lansync.common import NodeChunk
from lansync.util.file import (
    hash_path,
    file_checksum,
    read_file_chunks,
    read_chunk,
    write_chunk,
    create_file_placeholder,
)


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

    def transfer_chunk(self, src: Path, chunk: NodeChunk):
        data = read_chunk(src, chunk.offset, chunk.size)
        self.write_chunk(chunk, data)

    def store(self, session: Session, stored_node: Optional[StoredNode]) -> StoredNode:
        """
        TODO: maybe use:
            (StoredNode
                .insert(**kwargs)
                .on_conflict("replace")
                .execute()
            )
        TODO: clear unused chunks
        """
        chunks = self.chunks
        with atomic():
            if stored_node is not None:
                stored_node.checksum = self.checksum
                stored_node.size = self.size
                stored_node.local_modified_time = self.modified_time
                stored_node.local_created_time = self.created_time
                stored_node.ready = True
                stored_node.save()
            else:
                stored_node = StoredNode.create(
                    namespace=Namespace.for_session(session),
                    root_folder=RootFolder.for_session(session),
                    key=self.key,
                    path=self.path,
                    checksum=self.checksum,
                    size=self.size,
                    local_modified_time=self.modified_time,
                    local_created_time=self.created_time,
                    ready=True,
                )

            for chunk in chunks:
                NodeChunkModel.update_or_create(stored_node, chunk)

        return stored_node
