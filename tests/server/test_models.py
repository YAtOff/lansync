import random

import pytest

from faker import Faker, providers

from lansync_server.models import Sequence, all_models
from lansync_server.service import load_events, store_events
from lansync.common import NodeOperation, NodeChunk
from lansync.database import open_database


fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.misc)
fake.add_provider(providers.file)
fake.add_provider(providers.date_time)


def create_event(**overrides):
    operation = random.choice((NodeOperation.CREATE, NodeOperation.DELETE))
    events = {
        "key": fake.md5(),
        "operation": operation,
        "path": fake.file_path(),
        "timestamp": fake.date_time().isoformat(),
    }
    if operation == NodeOperation.CREATE:
        events.update({
            "checksum": fake.md5(),
            "size": fake.pyint(1, 1024),
            "chunks": [
                {"hash": fake.md5(), "size": fake.pyint(1, 128), "offset": fake.pyint(0, 1023)}
                for _ in range(random.randint(0, 10))
            ]
        })

    return {**events, **overrides}


@pytest.fixture()
def db():
    with open_database(":memory:", all_models):
        yield


def test_sequnece_increment(db):
    assert Sequence.increment("key") == 0
    assert Sequence.increment("key") == 1


def test_store_events(db):
    last_seq_num = store_events(
        fake.hostname(),
        [create_event() for _ in range(5)]
    )
    assert last_seq_num == 4


def test_load_events(db):
    src_events = [create_event() for _ in range(5)]
    namespace = fake.hostname()
    store_events(namespace, src_events)

    events = load_events(namespace, 2)
    assert [e["key"] for e in events] == [e["key"] for e in src_events[2:]]
