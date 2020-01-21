from contextlib import contextmanager
from typing import Callable


def force_bytes(s, encoding="utf-8", errors="strict"):
    if isinstance(s, bytes):
        if encoding == "utf-8":
            return s
        else:
            return s.decode("utf-8", errors).encode(encoding, errors)
    if isinstance(s, memoryview):
        return bytes(s)
    return str(s).encode(encoding, errors)


class ResourceManager:
    def __init__(self):
        self.handles = []

    def add(self, handle, close_fn: Callable):
        self.handles.append((handle, close_fn))
        return handle

    def ok(self):
        return all(h for h, _ in self.handles)

    def close(self):
        for h, close_fn in self.handles:
            if h:
                close_fn(h)


@contextmanager
def resource_manager():
    rm = ResourceManager()
    try:
        yield rm
    finally:
        rm.close()
