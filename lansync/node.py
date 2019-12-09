from __future__ import annotations

import enum
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from lansync.session import Session
from lansync.models import StoredNode
from lansync.util.file import hash_path, file_checksum, file_chunks_checksums


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


@dataclass
class NodeEvent:
    key: str
    operation: NodeOperation
    path: str
    timestamp: str
    checksum: Optional[str] = None
    parts: Optional[List[str]] = None
    sequence_number: Optional[int] = None


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
            _checksum=None
        )

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
    def parts(self) -> List[str]:
        return file_chunks_checksums(self.local_fspath)
