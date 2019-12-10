from __future__ import annotations

import json
import logging
import socket
import threading
import time
from random import randint
from typing import Dict, NamedTuple, List, Optional

from dynaconf import settings  # type: ignore
from pydantic import BaseModel


class DiscoveryMessage(BaseModel):
    device_id: str
    namespace: str
    port: int


class Peer(NamedTuple):
    address: str
    port: int


class PeerRegistry:
    def __init__(self):
        self.peers: Dict[str, List[Peer]] = {}
        self.lock = threading.RLock()

    def handle_discovery_message(self, address: str, msg: DiscoveryMessage) -> None:
        with self.lock:
            self.peers.setdefault(msg.namespace, [])
            peer = Peer(address=address, port=msg.port)
            if peer not in self.peers[msg.namespace]:
                self.peers[msg.namespace].append(peer)
                logging.info("[DISCOVERY] new peer joined: %r", peer)

    def choose(self, namespace: str) -> Optional[Peer]:
        with self.lock:
            peers = self.peers.get(namespace)
            if not peers:
                return None
            return peers[randint(0, len(peers))]


class Receiver:
    def __init__(self, device_id: str, peer_registry: PeerRegistry):
        self.device_id = device_id
        self.peer_registry = peer_registry

    def run(self):
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
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
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Set a timeout so the socket does not block indefinitely when trying to receive data.
        server.settimeout(0.2)
        while True:
            server.sendto(self.msg, ('<broadcast>', settings.DISCOVERY_PORT))
            logging.debug("[DISCOVERY] -> %s", self.msg)
            time.sleep(settings.DISCOVERY_PING_INTERVAL)

    @classmethod
    def run_in_thread(cls, msg: DiscoveryMessage):
        sender = Sender(msg)
        threading.Thread(target=sender.run, daemon=True).start()


def run_discovery_loop(device_id: str, namespace: str, port: int, peer_registry: PeerRegistry):
    Receiver.run_in_thread(device_id, peer_registry)
    Sender.run_in_thread(
        DiscoveryMessage(device_id=device_id, namespace=namespace, port=port)
    )
