from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import socket
import threading
import time
from typing import Dict

from dynaconf import settings  # type: ignore
from pydantic import BaseModel


class DiscoveryMessage(BaseModel):
    port: int
    namespace: str


@dataclass
class Client:
    port: int
    namespace: str


class ClientDB:
    def __init__(self):
        self.clients: Dict[str, Client] = {}
        self.lock = threading.RLock()

    def handle_discovery_message(self, address: str, message: DiscoveryMessage) -> None:
        with self.lock:
            client = Client(port=message.port, namespace=message.namespace)
            if address not in self.clients:
                logging.info("[DISCOVERY] new client joined: %r", client)
                self.clients[address] = client
            elif self.clients[address] != client:
                logging.info("[DISCOVERY] client %r reconfigured", client)
                self.clients[address] = client


class Receiver:
    def __init__(self, client_db: ClientDB):
        self.client_db = client_db

    def run(self):
        client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        client.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        client.bind(("", settings.DISCOVERY_PORT))
        while True:
            data, addr = client.recvfrom(1024)
            logging.debug("[DISCOVERY] %s <- %s", data, addr)

            self.client_db.handle_discovery_message(addr, DiscoveryMessage.parse_raw(data))

    @classmethod
    def run_in_thread(cls, client_db):
        receiver = Receiver(client_db)
        threading.Thread(target=receiver.run, daemon=True).start()


class Sender:
    def __init__(self, namespace: str):
        self.namespace = namespace

    def run(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        # Set a timeout so the socket does not block indefinitely when trying to receive data.
        server.settimeout(0.2)
        message = json.dumps(
            DiscoveryMessage(port=settings.DISCOVERY_PORT, namespace=self.namespace).dict()
        ).encode("utf-8")
        while True:
            server.sendto(message, ('<broadcast>', settings.DISCOVERY_PORT))
            logging.debug("[DISCOVERY] -> %s", message)
            time.sleep(settings.DISCOVERY_PING_INTERVAL)

    @classmethod
    def run_in_thread(cls, namespace: str):
        sender = Sender(namespace)
        threading.Thread(target=sender.run, daemon=True).start()


def loop(namespace: str):
    client_db = ClientDB()
    Receiver.run_in_thread(client_db)
    Sender.run_in_thread(namespace)

    while True:
        time.sleep(1)
