from __future__ import annotations

from dynaconf import settings  # type: ignore

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RootFolder:
    path: Path
    fspath: str

    @classmethod
    def create(cls, namespace: str) -> RootFolder:
        path = Path.cwd() / namespace
        return cls(path=path, fspath=os.fspath(path))


@dataclass
class Session:
    namespace: str
    root_folder: RootFolder
    remote_server_url: str

    @classmethod
    def create(cls, namespace: str) -> Session:
        root_folder = RootFolder.create(namespace)

        return cls(
            namespace=namespace,
            root_folder=root_folder,
            remote_server_url=settings.REMOTE_SERVER_URL
        )
