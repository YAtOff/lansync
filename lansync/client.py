import os
from pathlib import Path
from threading import RLock
import warnings
from typing import Dict, List, Optional, Iterable

import requests
from requests_toolbelt.adapters.host_header_ssl import HostHeaderSSLAdapter  # type: ignore
import urllib3.exceptions  # type: ignore

from lansync.peer import Peer
from lansync.market import Market
from lansync.node import RemoteNode

cert_file = os.fspath(Path.cwd() / "certs" / "alpha.crt")


def create_session():
    session = requests.Session()
    session.mount("https://", HostHeaderSSLAdapter())
    session.headers.update({"Host": "alpha"})
    session.verify = cert_file
    return session


class Client:
    def __init__(self, peer: Peer):
        self.peer = peer
        self.session = create_session()

    def download_chunk(self, namespace: str, hash: str) -> bytes:
        url = f"https://{self.peer.address}:{self.peer.port}/chunk/{namespace}/{hash}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)
            response = self.session.get(url, stream=False)
            response.raise_for_status()
        return response.content

    def exchange_market(self, market: Market) -> Optional[Market]:
        url = f"https://{self.peer.address}:{self.peer.port}/market/{market.namespace}/{market.key}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)
            response = self.session.post(url, data=market.dump(), stream=False)
        if response.status_code == 200:
            return Market.load(response.content)
        return None

    def exchange_node(self, remote_node: RemoteNode):
        url = f"https://{self.peer.address}:{self.peer.port}/node/{remote_node.namespace}"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", urllib3.exceptions.SubjectAltNameWarning)
            response = self.session.post(url, json=remote_node.dump(), stream=False)
            response.raise_for_status()


class ClientPool:
    clients: Dict[str, List[Client]]

    def __init__(self, clients_per_peer: int):
        self.clients_per_peer = clients_per_peer
        self.clients = {}
        self.lock = RLock()

    def aquire(self, peer: Peer) -> Optional[Client]:
        with self.lock:
            if peer.device_id not in self.clients:
                self.clients[peer.device_id] = [Client(peer) for _ in range(self.clients_per_peer)]
            try:
                return self.clients[peer.device_id].pop()
            except IndexError:
                return None

    def try_aquire_peers(self, peers: Iterable[Peer], max_count: int = 1) -> Iterable[Client]:
        aquired_count = 0
        for peer in peers:
            if aquired_count == max_count:
                break
            client = self.aquire(peer)
            if client is not None:
                yield client
                aquired_count += 1

    def release(self, client: Client):
        with self.lock:
            self.clients[client.peer.device_id].append(client)

    def remove(self, peer: Peer):
        with self.lock:
            self.clients.pop(peer.device_id, None)
