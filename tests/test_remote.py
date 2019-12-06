from dataclasses import asdict
import random
from unittest.mock import Mock

import pytest
from faker import Faker, providers
from playhouse.sqlite_udf import hostname

from lansync.database import open_database
from lansync.models import RemoteNode, Namespace, all_models
from lansync.common import NodeEvent, NodeOperation, NodeChunk
from lansync.remote import RemoteEventHandler, RemoteUrl

fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.misc)
fake.add_provider(providers.file)
fake.add_provider(providers.date_time)


def create_event(**overrides):
    operation = overrides.pop(
        "operation",
        random.choice((NodeOperation.CREATE, NodeOperation.DELETE))
    )
    event = {
        "key": fake.md5(),
        "operation": operation,
        "sequence_number": fake.pyint(1, 1000),
        "path": fake.file_path(),
        "timestamp": fake.date_time().isoformat(),
    }
    if operation == NodeOperation.CREATE:
        event.update({
            "checksum": fake.md5(),
            "size": fake.pyint(1, 1024),
            "chunks": [
                NodeChunk(fake.md5(), fake.pyint(1, 128), fake.pyint(0, 1023))
                for _ in range(random.randint(0, 10))
            ]
        })
    event.update(overrides)

    return NodeEvent(**event)


@pytest.fixture()
def db():
    with open_database(":memory:", all_models):
        yield


def test_remote_url():
    session = Mock(remote_server_url="http://example.com", namespace="ns")
    assert RemoteUrl(session).events() == "http://example.com/namespace/ns/events"
    assert RemoteUrl(session).events(foo="bar") == "http://example.com/namespace/ns/events?foo=bar"


def test_handle_create_event(db):
    namespace = fake.hostname()
    handler = RemoteEventHandler(Mock(namespace=namespace))
    event = create_event(operation=NodeOperation.CREATE)
    handler.handle(event)

    assert RemoteNode.select().count() == 1


def test_handle_delete_event(db):
    namespace = Namespace.by_name(fake.hostname())
    handler = RemoteEventHandler(Mock(namespace=namespace.name))
    event = create_event(operation=NodeOperation.CREATE)
    RemoteNode.create(namespace=namespace, **asdict(event))

    delete_event = create_event(key=event.key, operation=NodeOperation.DELETE)
    handler.handle(delete_event)

    assert RemoteNode.select().count() == 0


def test_get_max_sequence_number(db):
    namespace = fake.hostname()
    handler = RemoteEventHandler(Mock(namespace=namespace))
    seq_nums = [random.randint(1, 100) for _ in range(10)]
    for seq_num in seq_nums:
        event = create_event(
            operation=NodeOperation.CREATE,
            sequence_number=seq_num
        )
        handler.handle(event)

    max_seq_num = RemoteNode.max_sequence_number(Namespace.by_name(namespace))
    assert max_seq_num == max(seq_nums)
