from __future__ import annotations

from dataclasses import dataclass, asdict
import hashlib
from itertools import chain
from pathlib import Path
import os
from typing import Union, List, Optional, Iterable, Tuple

from dynaconf import settings  # type: ignore

from rschunks.delta import DeltaCommand, LiteralDeltaCommand, CopyDeltaCommand


@dataclass
class Chunk:
    offset: int
    size: int
    hash: Optional[str] = None

    @classmethod
    def from_range(cls, start: int, end: int, normal_size: int = settings.CHUNK_SIZE) -> Iterable[Chunk]:
        offset = start
        while offset < end:
            chunk_size = min(normal_size, end - offset)
            yield Chunk(offset=offset, size=chunk_size)
            offset += chunk_size

    def merge(self, other: Chunk) -> Chunk:
        assert self.offset + self.size == other.offset
        return Chunk(offset=self.offset, size=self.size + other.size)

    def split(self, min_size: int = settings.CHUNK_SIZE) -> Tuple[Chunk, Optional[Chunk]]:
        if self.size > min_size:
            return (
                Chunk(offset=self.offset, size=min_size),
                Chunk(offset=self.offset + min_size, size=self.size - min_size),
            )
        else:
            return self, None


def read_chunks_from_fd(
    fd,
    hash_func: str = settings.CHUNK_HASH_FUNC,
    chunk_size: int = settings.CHUNK_SIZE,
) -> Iterable[Chunk]:
    offset = 0
    while True:
        hash = hashlib.new(hash_func)
        data = fd.read(chunk_size)
        size = len(data)
        if size == 0:
            break
        hash.update(data)
        yield Chunk(offset, size, hash.hexdigest())
        offset += size
        if size < chunk_size:
            break


def read_chunks_from_file(
    path: Union[Path, str],
    hash_func: str = settings.CHUNK_HASH_FUNC,
    chunk_size: int = settings.CHUNK_SIZE,
) -> Iterable[Chunk]:
    with open(path, "rb") as fd:
        yield from read_chunks_from_fd(fd, hash_func=hash_func, chunk_size=chunk_size)


def fill_chunks_from_fd(
    chunks: Iterable[Chunk],
    fd,
    hash_func: str = settings.CHUNK_HASH_FUNC
):
    for chunk in chunks:
        fd.seek(chunk.offset, os.SEEK_SET)
        data = fd.read(chunk.size)
        assert len(data) == chunk.size
        hash = hashlib.new(hash_func)
        hash.update(data)
        chunk.hash = hash.hexdigest()


def fill_chunks_from_file(
    chunks: Iterable[Chunk],
    path: Union[Path, str],
    hash_func: str = settings.CHUNK_HASH_FUNC,
):
    with open(path, "rb") as fd:
        fill_chunks_from_fd(chunks, fd, hash_func=hash_func)


def serialize_file_chunks(chunks: Iterable[Chunk]):
    return [asdict(chunk) for chunk in chunks]


def deserialize_file_chunks(data) -> List[Chunk]:
    return [Chunk(**d) for d in data]


def merge_chunks(
    chunks: Iterable[Chunk], normal_chunk_size: int = settings.CHUNK_SIZE
) -> Iterable[Chunk]:
    prev = None
    for chunk in chunks:
        if chunk.size == normal_chunk_size:
            if prev is not None:
                yield prev
                prev = None
            yield chunk
        else:
            prev = prev.merge(chunk) if prev is not None else chunk  # type: ignore
    if prev is not None:
        yield prev


def split_chunks(chunks: Iterable[Chunk], min_size: int = settings.CHUNK_SIZE) -> Iterable[Chunk]:
    for chunk in chunks:
        chunk, next_chunk = chunk.split(min_size=min_size)
        yield chunk
        while next_chunk is not None:
            chunk, next_chunk = chunk.split()
            yield chunk


def handle_delta_commands(
    delta_commands: List[DeltaCommand],
    normal_chunk_size: int = settings.CHUNK_SIZE
) -> Iterable[Chunk]:
    offset = 0
    for cmd in delta_commands:
        if isinstance(cmd, LiteralDeltaCommand):
            yield Chunk(offset=offset, size=cmd.length, hash=None)
            offset += cmd.length
        elif isinstance(cmd, CopyDeltaCommand):
            yield from Chunk.from_range(
                offset, offset + cmd.length, normal_size=normal_chunk_size
            )
            offset += cmd.length


def update_chunks(
    delta_commands: List[DeltaCommand],
    filename: str, normal_chunk_size: int = settings.CHUNK_SIZE
) -> Iterable[Chunk]:
    chunks = list(split_chunks(
        merge_chunks(
            handle_delta_commands(
                delta_commands,
                normal_chunk_size=normal_chunk_size
            ),
            normal_chunk_size=normal_chunk_size
        ),
        min_size=normal_chunk_size
    ))
    fill_chunks_from_file(chunks, filename)
    return chunks


def diff_chunks(old: List[Chunk], new: List[Chunk]):
    sizes = {c.hash: c.size for c in chain(old, new)}
    oldset = {c.hash for c in old}
    newset = {c.hash for c in new}
    return {
        "new_size": sum(sizes[c] for c in newset),
        "old_size": sum(sizes[c] for c in oldset),
        "preserved": sum(sizes[c] for c in (oldset & newset)),
        "deleted": sum(sizes[c] for c in (oldset - newset)),
        "added": sum(sizes[c] for c in (newset - oldset)),
    }
