import pytest
from faker import Faker, providers

from lansync import models
from lansync.database import open_database
from lansync.market import ChunkSet, Market, MarketRepo

fake = Faker()
fake.add_provider(providers.misc)
fake.add_provider(providers.internet)
fake.add_provider(providers.file)


@pytest.fixture()
def db():
    with open_database(":memory:", models.all_models):
        yield


def test_serialize_market():
    market = Market(
        namespace=fake.user_name(), key=fake.md5(),
        peers={fake.uuid4(): ChunkSet(1, fake.binary(1)) for _ in range(1)},
    )
    data = market.dump()
    new_market = Market.load(data)
    assert market == new_market


def test_create_empty_chunks_set():
    chunk_set = ChunkSet.empty(10)
    assert not any(chunk_set.has(i) for i in range(10))


def test_create_full_chunks_set():
    chunk_set = ChunkSet.full(10)
    assert chunk_set.has_all()


@pytest.mark.parametrize(
    "marked,position,result", [([0,], 0, True), ([], 0, False), ([0, 9], 9, True)]
)
def test_chunk_available(marked, position, result):
    chunk_set = ChunkSet.empty(16)
    for i in marked:
        chunk_set = chunk_set.mark(i)
    assert chunk_set.has(position) == result


def test_merge_chunks():
    cs = ChunkSet.empty(16).mark(1).merge(ChunkSet.empty(16).mark(2))
    assert cs.has(1)
    assert cs.has(2)


def test_merge_chunks_different_sizes():
    cs = ChunkSet.empty(8).mark(1).merge(ChunkSet.empty(16).mark(8))
    assert cs.has(1)
    assert cs.has(8)


def test_diff_chunks():
    cs = ChunkSet.empty(16).mark(1).mark(2).diff(ChunkSet.empty(16).mark(2))
    assert cs.has(1)
    assert not cs.has(2)


@pytest.mark.parametrize("marked,result", [([0,], True), ([], False), ([0, 3, 7], True)])
def test_pick_random(marked, result):
    chunk_set = ChunkSet.empty(16)
    for i in marked:
        chunk_set = chunk_set.mark(i)
    assert (chunk_set.pick_random() in marked) == result


def test_create_initial_markets():
    src = fake.uuid4()
    peer = fake.uuid4()
    market = Market.for_file_provider(
        namespace=fake.user_name(), key=fake.md5(),
        peers=[peer], chunks_count=1, src=src
    )
    assert market.peers[src].has(0)
    assert not market.peers[peer].has(0)


def test_merge_markets():
    defaults = {"namespace": fake.user_name(), "key": fake.md5()}
    device1, device2 = fake.uuid4(), fake.uuid4()
    market1 = Market(peers={device1: ChunkSet.empty(16).mark(1)}, **defaults)
    market2 = Market(
        peers={device1: ChunkSet.empty(16).mark(2), device2: ChunkSet.empty(16).mark(1)}, **defaults
    )
    market1.merge(market2)
    assert market1.peers[device1].has(1)
    assert market1.peers[device1].has(2)
    assert market1.peers[device2].has(1)


def test_market_repo(db):
    namespace = fake.user_name()
    key = fake.md5()
    repo = MarketRepo()
    market1 = Market(
        namespace=namespace, key=key,
        peers={fake.uuid4(): ChunkSet(1, fake.binary(1)) for _ in range(1)},
    )
    market1 = repo.save(market1)
    market2 = Market(
        namespace=namespace, key=key,
        peers={fake.uuid4(): ChunkSet(1, fake.binary(1)) for _ in range(1)},
    )
    market2 = repo.save(market2)
    market3 = MarketRepo().load(namespace, key)

    assert market2 == market3
    assert len(market2.peers) == 2
