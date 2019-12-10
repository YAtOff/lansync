from __future__ import annotations

from dynaconf import settings  # type: ignore

import os
from dataclasses import dataclass
from pathlib import Path

from lansync.discovery import PeerRegistry


@dataclass
class RootFolder:
    path: Path
    fspath: str

    @classmethod
    def create(cls, fspath: str) -> RootFolder:
        path = Path(fspath).resolve()
        return cls(path=path, fspath=os.fspath(path))


@dataclass
class Session:
    namespace: str
    root_folder: RootFolder
    remote_server_url: str
    peer_registry: PeerRegistry

    @classmethod
    def create(cls, namespace: str, root_folder: str) -> Session:
        return cls(
            namespace=namespace,
            root_folder=RootFolder.create(root_folder),
            remote_server_url=settings.REMOTE_SERVER_URL,
            peer_registry=PeerRegistry()
        )
