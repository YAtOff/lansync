from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime
from random import randint
from typing import Set, Optional, Sequence


@dataclass
class Peer:
    address: str
    port: int
    device_id: str
    timestamp: datetime = field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def update(self, address: str, port: int):
        if (self.address, self.port) != (address, port):
            self.address = address
            self.port = port
            logging.info("[DISCOVERY] peer changed location: %r", self)
        self.timestamp = datetime.now()


class PeerRegistry:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.lock = threading.RLock()

    def live_peers(self, namespace: str) -> Sequence[Peer]:
        raise NotImplementedError

    def choose(self, namespace: str) -> Optional[Peer]:
        with self.lock:
            live_peers = self.live_peers(namespace)
            return live_peers[randint(0, len(live_peers) - 1)] if live_peers else None

    def iter_peers(self, namespace: str):
        checked_peers: Set[str] = set()
        while True:
            live_peers = self.live_peers(namespace)
            peers = [p for p in live_peers if p.device_id not in checked_peers]
            if not peers:
                return

            peer = peers[randint(0, len(peers) - 1)]
            yield peer
            checked_peers.add(peer.device_id)

    def empty(self, namespace: str) -> bool:
        return len(self.live_peers(namespace)) == 0
