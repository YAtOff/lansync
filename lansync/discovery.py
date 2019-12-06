from __future__ import annotations

import json
import logging
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from random import randint
from typing import Dict, Set, Optional, Sequence, List

from dynaconf import settings  # type: ignore
from pydantic import BaseModel


class DiscoveryMessage(BaseModel):
    device_id: str
    namespace: str
    port: int


@dataclass
class Peer:
    address: str
    port: int
    device_id: str
    timestamp: datetime = field(init=False)

    def __post_init__(self):
        self.timestamp = datetime.now()

    def touch(self):
        self.timestamp = datetime.now()


class PeerRegistry:
    def __init__(self):
        self.peers: Dict[str, Dict[str, Peer]] = {}
        self.lock = threading.RLock()

    def handle_discovery_message(self, address: str, msg: DiscoveryMessage) -> None:
        with self.lock:
            self.peers.setdefault(msg.namespace, {})
            peer = self.peers[msg.namespace].get(msg.device_id, None)
            if peer is None:
                peer = Peer(address, msg.port, msg.device_id)
                self.peers[msg.namespace][msg.device_id] = peer
                logging.info("[DISCOVERY] new peer joined: %r", peer)
            else:
                peer.touch()

    def peers_for_namespace(self, namespace: str) -> List[Peer]:
        return list(self.peers.get(namespace, {}).values())

    def choose(self, namespace: str) -> Optional[Peer]:
        with self.lock:
            live_peers = self.live_peers(namespace)
            return live_peers[randint(0, len(live_peers) - 1)] if live_peers else None

    def live_peers(self, namespace: str) -> Sequence[Peer]:
        now = datetime.now()
        return [
            p
            for p in self.peers.get(namespace, {}).values()
            if now - p.timestamp < timedelta(minutes=5)
        ]

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

    @property
    def empty(self) -> bool:
        return len(self.peers) == 0


class Receiver:
    def __init__(self, device_id: str, peer_registry: PeerRegistry):
        self.device_id = device_id
        self.peer_registry = peer_registry

    def run(self):
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if sys.platform == "linux":
            client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        client.bind(("", settings.DISCOVERY_PORT))
        while True:
            data, addr = client.recvfrom(1024)
            logging.debug("[DISCOVERY] %s <- %s", data, addr)

            msg: DiscoveryMessage = DiscoveryMessage.parse_raw(data)
            if msg.device_id != self.device_id:
                self.peer_registry.handle_discovery_message(addr[0], msg)

    @classmethod
    def run_in_thread(cls, device_id: str, peer_registry: PeerRegistry):
        receiver = Receiver(device_id, peer_registry)
        threading.Thread(target=receiver.run, daemon=True).start()


class Sender:
    def __init__(self, msg: DiscoveryMessage):
        self.msg = json.dumps(msg.dict()).encode("utf-8")

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if sys.platform == "linux":
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Set a timeout so the socket does not block indefinitely when trying to receive data.
        server.settimeout(0.2)
        while True:
            server.sendto(self.msg, ("<broadcast>", settings.DISCOVERY_PORT))
            logging.debug("[DISCOVERY] -> %s", self.msg)
            time.sleep(settings.DISCOVERY_PING_INTERVAL)

    @classmethod
    def run_in_thread(cls, msg: DiscoveryMessage):
        sender = Sender(msg)
        threading.Thread(target=sender.run, daemon=True).start()


def run_discovery_loop(device_id: str, namespace: str, port: int, peer_registry: PeerRegistry):
    Receiver.run_in_thread(device_id, peer_registry)
    Sender.run_in_thread(DiscoveryMessage(device_id=device_id, namespace=namespace, port=port))
