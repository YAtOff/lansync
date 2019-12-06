from __future__ import annotations

from contextlib import contextmanager
import hashlib
import logging
import os
import os.path
from pathlib import Path
import tempfile
from typing import Generator, Optional, Dict, List, Union, Tuple


def iter_folder(folder: Path) -> Generator[Path, None, None]:
    for p in folder.iterdir():
        if p.is_file():
            yield p
        elif p.is_dir():
            yield from iter_folder(p)


def hash_path(path: str) -> str:
    return hashlib.new("md5", path.encode("utf-8")).hexdigest()


@contextmanager
def create_temp_file():
    fd, path = tempfile.mkstemp()
    os.close(fd)
    yield path
    try:
        os.unlink(path)
    except OSError:
        pass


def create_file_placeholder(path: Path, size: int) -> None:
    if not path.parent.exists():
        path.parent.mkdir(parents=True)
    with open(path, "wb") as f:
        f.seek(size - 1)
        f.write(b"\0")


def read_chunk(path: Path, offset: int, size: int) -> bytes:
    with open(path, "rb") as file:
        file.seek(offset, os.SEEK_SET)
        return file.read(size)


def write_chunk(path: Path, data: bytes, offset: int) -> None:
    with open(path, "r+b") as file:
        file.seek(offset, os.SEEK_SET)
        file.write(data)


def file_checksum(file_name: str, hash_func: str = "md5") -> Optional[str]:
    try:
        hash = hashlib.new(hash_func)
        with open(file_name, "rb") as f:
            data = f.read(1000000)
            while len(data) > 0:
                hash.update(data)
                data = f.read(1000000)
        return hash.hexdigest()
    except IOError:
        logging.error(u"[FILE] Error calculating checksum", exc_info=True)
        return None


def buffer_checksum(buffer: bytes, hash_func: str = "md5") -> str:
    hash = hashlib.new(hash_func)
    hash.update(buffer)
    return hash.hexdigest()


def read_file_chunks(
    path: Union[Path, str], hash_func: str = "md5", chunk_size: int = 1024 * 1024
) -> List[Tuple[str, int, int]]:
    chunks: List[Tuple[str, int, int]] = []
    try:
        with open(path, "rb") as f:
            offset = 0
            while True:
                hash = hashlib.new(hash_func)
                data = f.read(chunk_size)
                size = len(data)
                if size == 0:
                    break
                hash.update(data)
                chunks.append((hash.hexdigest(), size, offset))
                offset += size
                if size < chunk_size:
                    break
    except IOError:
        logging.error(u"[FILE] Error calculating checksum", exc_info=True)
    return chunks


def get_stats(path: str) -> Dict:
    stats = Path(path).stat()
    return {"size": stats.st_size / (1024 * 1024)}
