from pathlib import Path
import random
import string


import pytest
from faker import Faker, providers

from lansync.models import StoredNode, RemoteNode, Namespace
from lansync.node import LocalNode
from lansync.common import NodeChunk
from lansync.sync_action import (
    SyncAction,
    download,
    upload,
    delete_local,
    delete_remote,
    save_stored,
    delete_stored,
    conflict,
    nop,
)
from lansync.sync_logic import handle_node
from lansync.util.file import hash_path


class Bunch:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.date_time)
fake.add_provider(providers.file)


generate = Bunch(
    path=lambda: fake.file_path().strip("/"),
    key=lambda: "".join([random.choice("abcdef0123456789") for i in range(32)]),
    checksum=lambda: fake.md5(),
    chunks=lambda: [
        NodeChunk(fake.md5(), fake.pyint(1, 128), fake.pyint(0, 1023))
        for _ in range(random.randint(0, 10))
    ],
    datetime=lambda: fake.date_time(),
    timestamp=lambda: random.randint(1, 1000000000),
    iso_timestamp=lambda: fake.date_time().isoformat(),
    size=lambda: random.randint(1, 1000000000),
    sequence_number=lambda: random.randint(1, 1000000000)
)


class Generator:
    def __init__(self, factory):
        self.factory = factory
        self.value = factory()

    def same(self):
        return self.value

    def new(self):
        self.value = self.factory()
        return self.value


checksum = Generator(generate.checksum)
modified_time = Generator(generate.timestamp)
created_time = Generator(generate.timestamp)


class FileGenerator:
    gen = Bunch(
        path=Generator(generate.path),
        checksum=Generator(generate.checksum),
        modified_time=Generator(generate.timestamp),
        created_time=Generator(generate.timestamp),
        size=Generator(generate.size),
    )

    def remote(self, deleted=False, **extra_attrs):
        attrs = {
            **self.base_attrs,
            "namespace": self.namespace,
            "key": self.key,
            "checksum": self.checksum,
            "chunks": generate.chunks(),
            "timestamp": generate.iso_timestamp(),
            "size": self.size,
            **extra_attrs,
        }
        return RemoteNode(**attrs)

    def local(self, **extra_attrs):
        attrs = {
            **self.base_attrs,
            "root_folder": Path(generate.path()),
            "modified_time": self.modified_time,
            "created_time": self.created_time,
            "_checksum": self.checksum,
            "size": self.size,
            **extra_attrs,
        }
        return LocalNode(**attrs)

    def stored(self, **extra_attrs):
        attrs = {
            **self.base_attrs,
            "key": self.key,
            "namespace": self.namespace,
            "checksum": self.checksum,
            "local_modified_time": self.modified_time,
            "local_created_time": self.created_time,
            "size": self.size,
            "ready": True,
            **extra_attrs,
        }
        return StoredNode(**attrs)

    def new(self):
        self.path = self.gen.path.new()
        self.key = hash_path(self.path)
        self.checksum = self.gen.checksum.new()
        self.modified_time = self.gen.modified_time.new()
        self.created_time = self.gen.created_time.new()
        self.size = self.gen.size.new()
        return self

    namespace = Namespace(name="namespace")

    @property
    def base_attrs(self):
        return {
            "path": self.path,
        }


file = FileGenerator()


@pytest.mark.parametrize(
    "number,remote,local,stored,expected_action_factory",
    [
        (1, None, None, None, lambda r, l, s: nop()),
        (
            2, None, None, file.new().stored(),
            lambda r, l, s: delete_stored(s)
        ),
        (
            3, None, file.new().local(), None,
            lambda r, l, s: upload(l, s)
        ),
        (
            4, None, file.new().local(), file.stored(),
            lambda r, l, s: delete_local(l, s)
        ),
        (
            5, file.new().remote(), None, None,
            lambda r, l, s: download(r, s)
        ),
        (
            6, file.new().remote(), None, file.stored(),
            lambda r, l, s: delete_remote(r, s)
        ),
        (
            7, file.new().remote(), file.local(), None,
            lambda r, l, s: save_stored(r, l)
        ),
        (
            8, file.new().remote(), file.local(_checksum=checksum.new()), None,
            lambda r, l, s: conflict(r, l, s)
        ),
        (
            9, file.new().remote(), file.local(), file.stored(),
            lambda r, l, s: nop()
        ),
        (
            10,
            file.new().remote(),
            file.local(modified_time=modified_time.new(), _checksum=checksum.new()),
            file.stored(),
            lambda r, l, s: upload(l, s)
        ),
        (
            11, file.new().remote(checksum=checksum.new()), file.local(), file.stored(),
            lambda r, l, s: download(r, s)
        ),
        (
            12,
            file.new().remote(checksum=checksum.new()),
            file.local(modified_time=modified_time.new(), _checksum=checksum.same()),
            file.stored(),
            lambda r, l, s: save_stored(r, l)
        ),
        (
            13,
            file.new().remote(checksum=checksum.new()),
            file.local(modified_time=modified_time.new(), _checksum=checksum.new()),
            file.stored(),
            lambda r, l, s: conflict(r, l, s)
        ),
        (
            14, file.new().remote(), file.local(), file.stored(ready=False),
            lambda r, l, s: download(r, s)
        ),
    ]
)
def test_handle_node(number, remote, local, stored, expected_action_factory):
    action = handle_node(remote, local, stored)
    expected_action = expected_action_factory(remote, local, stored)
    assert repr(action) == repr(expected_action)
