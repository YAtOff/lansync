import enum
from dataclasses import dataclass
from typing import List, Optional

from lansync.util.file import buffer_checksum


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
class NodeChunk:
    offset: int
    size: int
    hash: str

    def check(self, data: bytes):
        assert len(data) == self.size and buffer_checksum(data) == self.hash


@dataclass
class NodeEvent:
    key: str
    operation: NodeOperation
    path: str
    timestamp: str
    checksum: Optional[str] = None
    size: Optional[int] = None
    chunks: Optional[List[NodeChunk]] = None
    signature: Optional[str] = None
    sequence_number: Optional[int] = None
