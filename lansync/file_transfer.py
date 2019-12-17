from contextlib import contextmanager
import os
import shutil
from pathlib import Path

import requests
from requests_toolbelt.adapters.host_header_ssl import HostHeaderSSLAdapter  # type: ignore

from lansync.models import RemoteNode
from lansync.session import Session
from lansync.util.file import create_temp_file


cert_file = os.fspath(Path.cwd() / "certs" / "alpha.crt")


@contextmanager
def create_session():
    with requests.Session() as session:
        session.mount('https://', HostHeaderSSLAdapter())
        session.headers.update({"Host": "alpha"})
        session.verify = cert_file
        yield session


def download_from_peer(remote_node: RemoteNode, dest: Path, session: Session) -> bool:
    for peer in session.peer_registry.iter_peers(session.namespace):
        url = f"https://{peer.address}:{peer.port}/content/{session.namespace}/{remote_node.key}"
        with create_session() as s:
            response = s.head(url)
            if response.status_code == 200:
                download_file(s, url, dest)
                return True

    return False


def download_file(session, url: str, dest: Path):
    with create_temp_file() as file_path:
        with open(file_path, "wb") as file:
            response = session.get(url, stream=True)
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
