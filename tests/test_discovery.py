from datetime import datetime

from faker import Faker, providers
from playhouse.sqlite_udf import hostname

from lansync.discovery import DiscoveryMessage, PeerRegistry, Peer

fake = Faker()
fake.add_provider(providers.internet)
fake.add_provider(providers.misc)
fake.add_provider(providers.date_time)


def create_peer_params():
    return (
        fake.md5(),
        fake.hostname(),
        fake.ipv4(),
        fake.pyint(min_value=1024, max_value=65535)
    )


def test_discovery_message_handling():
    registry = PeerRegistry()
    device_id, namespace, ip, port = create_peer_params()
    registry.handle_discovery_message(
        ip, DiscoveryMessage(device_id=device_id, namespace=namespace, port=port)
    )

    assert (ip, port) in registry.peers[namespace]


def test_peer_choose():
    registry = PeerRegistry()
    device_id, namespace, ip, port = create_peer_params()
    registry.handle_discovery_message(
        ip, DiscoveryMessage(device_id=device_id, namespace=namespace, port=port)
    )

    peer = registry.choose(namespace)
    assert peer.key == (ip, port)

    registry.peers[namespace][(ip, port)].timestamp = datetime.min
    assert registry.choose(namespace) is None


def test_peer_iteration():
    registry = PeerRegistry()
    namespace = fake.hostname()
    peer_params = [create_peer_params() for _ in range(5)]
    for device_id, _, ip, port in peer_params:
        registry.handle_discovery_message(
            ip, DiscoveryMessage(device_id=device_id, namespace=namespace, port=port)
        )

    assert len(list(registry.iter_peers(namespace))) == 5
