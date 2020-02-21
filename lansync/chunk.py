from base64 import b64decode
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List

import librsync
from rschunks.chunk import read_chunks_from_file, update_chunks
from rschunks.delta import parse_delta_from_file

from lansync.util.file import create_temp_file, buffer_checksum


@dataclass
class NodeChunk:
    offset: int
    size: int
    hash: str

    def check(self, data: bytes):
        assert len(data) == self.size and buffer_checksum(data) == self.hash


def calc_initial_chunks(path: str) -> List[NodeChunk]:
    return [NodeChunk(**asdict(c)) for c in read_chunks_from_file(path)]


def calc_new_chunks(path: str, signature: str) -> List[NodeChunk]:
    with create_temp_file() as signature_file, create_temp_file() as delta_file:
        Path(signature_file).write_bytes(b64decode(signature))
        librsync.delta_from_paths(signature_file, path, delta_file)
        delta_commands = list(parse_delta_from_file(delta_file))
        return [NodeChunk(**asdict(c)) for c in update_chunks(delta_commands, path)]
