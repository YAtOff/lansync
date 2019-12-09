from contextlib import contextmanager
import hashlib
import logging
import os
import os.path
from pathlib import Path
import tempfile
from typing import Generator, Optional, Dict, List


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


def file_chunks_checksums(
    file_name: str,
    hash_func: str = "md5",
    chunk_size: int = 1024 * 1024
) -> List[str]:
    hashes = []
    try:
        with open(file_name, "rb") as f:
            while True:
                hash = hashlib.new(hash_func)
                data = f.read(chunk_size)
                hash.update(data)
                hashes.append(hash.hexdigest())
                if len(data) < chunk_size:
                    break
    except IOError:
        logging.error(u"[FILE] Error calculating checksum", exc_info=True)
    return hashes


def get_stats(path: str) -> Dict:
    stats = Path(path).stat()
    return {"size": stats.st_size / (1024 * 1024)}
