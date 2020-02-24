from faker import Faker, providers

from lansync.client import ClientPool
from lansync.peer import Peer


fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.misc)
fake.add_provider(providers.date_time)


def create_peer():
    return Peer(
        fake.ipv4(),
        fake.pyint(min_value=1024, max_value=65535),
        fake.hostname()
    )


def test_aquire_client_creates_new():
    pool = ClientPool(1)
    assert pool.aquire(create_peer()) is not None


def test_aquire_fails_all_clients_are_busy():
    pool = ClientPool(1)
    peer = create_peer()
    assert pool.aquire(peer) is not None
    assert pool.aquire(peer) is None


def test_release_recovers_clients():
    pool = ClientPool(1)
    peer = create_peer()
    client = pool.aquire(peer)
    assert client is not None
    assert pool.aquire(peer) is None
    pool.release(client)
    assert pool.aquire(peer) is not None

