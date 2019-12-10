import shutil
from pathlib import Path
from typing import Set, Optional

import requests

from lansync.models import RemoteNode
from lansync.session import Session
from lansync.discovery import Peer
from lansync.util.file import create_temp_file


def download_from_peer(remote_node: RemoteNode, dest: Path, session: Session) -> bool:
    dummy_peer = Peer(address="", port=0)
    checked_peers: Set[Peer] = set((dummy_peer,))
    peer: Optional[Peer] = dummy_peer
    while peer in checked_peers:
        peer = session.peer_registry.choose(session.namespace)
        if peer is not None:
            url = f"http://{peer.address}:{peer.port}/content/{session.namespace}/{remote_node.key}"
            response = requests.head(url)
            if response.status_code == 200:
                download_file(url, dest)
                return True
        else:
            return False

    return False


def download_file(url: str, dest: Path):
    with create_temp_file() as file_path:
        with open(file_path, "wb") as file:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            content_length = response.headers.get("content-length")

            if content_length is None:  # no content length header
                file.write(response.content)
            else:
                bytes_transferred = 0
                for data in response.iter_content(chunk_size=16 * 4096):
                    bytes_transferred += len(data)
                    file.write(data)

        if not dest.parent.exists():
            dest.parent.mkdir(parents=True)
        shutil.move(file_path, dest)
