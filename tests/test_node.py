import os
import tempfile
from os.path import normcase
from pathlib import Path
from unittest.mock import Mock

import pytest
from faker import Faker, providers

from lansync.database import open_database
from lansync.discovery import Peer
from lansync.models import Chunk, NodeChunk, StoredNode, all_models
from lansync.node import LocalNode, store_new_node
from lansync.session import RootFolder

fake = Faker()
fake.add_provider(providers.misc)
fake.add_provider(providers.internet)


@pytest.fixture()
def db():
    with open_database(":memory:", all_models):
        yield


@pytest.fixture()
def file_manager():
    class FileManager:
        files = []

        @property
        def root(self):
            return tempfile.gettempdir()

        def create_file(self, size):
            fd, path = tempfile.mkstemp()
            os.close(fd)
            with open(path, "wb") as f:
                f.write(fake.binary(size))
            self.files.append(path)
            return Path(path)

        def clean(self):
            for path in self.files:
                try:
                    os.unlink(path)
                except OSError:
                    pass

    file_manager = FileManager()
    yield file_manager
    file_manager.clean()


@pytest.fixture
def session(file_manager):
    namespace = fake.user_name()
    peer = Peer(
        address=fake.ipv4(),
        port=fake.pyint(min_value=1024, max_value=65535),
        device_id=fake.uuid4()
    )
    return Mock(
        namespace=namespace,
        root_folder=RootFolder.create(file_manager.root),
        device_id=fake.uuid4(),
        peer_registry=Mock(peers={namespace: {(peer.address, peer.port): peer}})
    )


def test_find_chunk_by_hash(db, file_manager, session):
    local_node = LocalNode.create(
        file_manager.create_file(1024 * 1024 * 2 + 1024), session
    )
    full_node = store_new_node(local_node, session, None)

    chunk1 = full_node.all_chunks[0]
    chunk2, _ = NodeChunk.find(session.namespace, chunk1.hash)
    assert chunk1 == chunk2

    assert full_node.stored_node.chunks == full_node.all_chunks
