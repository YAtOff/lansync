from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dynaconf import settings  # type: ignore

from lansync.discovery import PeerRegistry
from lansync.util.lazy_object import LazyObject


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
    device_id: str
    peer_registry: PeerRegistry
    market_repo: Any
    client_pool: Any

    @classmethod
    def create(cls, namespace: str, root_folder: str, device_id: str) -> Session:
        from lansync.market import MarketRepo
        from lansync.client import ClientPool

        return cls(
            namespace=namespace,
            root_folder=RootFolder.create(root_folder),
            remote_server_url=settings.REMOTE_SERVER_URL,
            device_id=device_id,
            peer_registry=PeerRegistry(),
            market_repo=MarketRepo(),
            client_pool=ClientPool(settings.CLIENTS_PER_PEER)
        )


instance = LazyObject()
