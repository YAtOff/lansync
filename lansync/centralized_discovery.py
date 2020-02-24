from __future__ import annotations

import json
import threading
import time
from dataclasses import asdict
from typing import Sequence

from cached_property import cached_property  # type: ignore
from dynaconf import settings  # type: ignore
import redis  # type: ignore

from lansync.peer import Peer, PeerRegistry


redis_client = redis.Redis()


class CentrailzedPeerRegistry(PeerRegistry):
    def live_peers(self, namespace: str) -> Sequence[Peer]:
        peers = []
        cursor = 0
        new_cursor = -1
        while new_cursor != 0:
            new_cursor, keys = redis_client.scan(cursor, f"peer:{namespace}*")  # type: ignore
            cursor = new_cursor
            values = redis_client.mget(keys)
            peers.extend([Peer(**json.loads(v)) for v in values])

        return [p for p in peers if p.device_id != self.device_id]


class Sender:
    def __init__(self, namespace: str, current_peer: Peer):
        self.namespace = namespace
        self.device_id = current_peer.device_id
        self.current_peer = current_peer

    def run(self):
        while True:
            redis_client.set(
                f"peer:{self.namespace}:{self.device_id}",
                self.peer_data,
                ex=settings.DISCOVERY_PING_INTERVAL * 2
            )
            time.sleep(settings.DISCOVERY_PING_INTERVAL)

    @cached_property
    def peer_data(self) -> bytes:
        return json.dumps({
            k: v for k, v in asdict(self.current_peer).items() if k != "timestamp"
        }).encode("utf-8")

    @classmethod
    def run_in_thread(cls, namespace: str, current_peer: Peer):
        sender = Sender(namespace, current_peer)
        threading.Thread(target=sender.run, daemon=True).start()


def run_discovery_loop(device_id: str, namespace: str, port: int, peer_registry: PeerRegistry):
    Sender.run_in_thread(namespace, Peer(
        address="127.0.0.1", device_id=device_id, port=port,
    ))
