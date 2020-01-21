import random
from functools import reduce

from faker import Faker, providers


from rschunks.chunk import (
    merge_chunks,
    split_chunks,
    handle_delta_commands,
    diff_chunks,
    Chunk,
)
from rschunks.delta import LiteralDeltaCommand, CopyDeltaCommand


fake = Faker()
fake.add_provider(providers.misc)

CHUNK_SIZE = 1024


def make_chunk(prev):
    size_ranges = [
        (0, CHUNK_SIZE),
        (CHUNK_SIZE, CHUNK_SIZE),
        (CHUNK_SIZE, CHUNK_SIZE * 10),
    ]
    size_range = random.choice(size_ranges)
    size = random.randint(*size_range)
    offset = 0 if prev is None else prev.offset + prev.size
    return Chunk(offset=offset, size=size)


def make_chunks(count):
    return reduce(
        lambda chunks, _: chunks + [make_chunk(chunks[-1])], range(count - 1), [make_chunk(None)]
    )


def test_merge_chunks():
    chunks = make_chunks(10)
    merged = list(merge_chunks(chunks, normal_chunk_size=CHUNK_SIZE))

    assert all(any(c.size >= CHUNK_SIZE for c in pair) for pair in zip(merged, merged[1:]))


def test_split_chunks():
    chunks = make_chunks(10)
    merged = merge_chunks(chunks, normal_chunk_size=CHUNK_SIZE)
    splitted = list(split_chunks(merged, min_size=CHUNK_SIZE))

    assert all(c.size <= CHUNK_SIZE for c in splitted)


def test_make_chunks_from_range():
    assert list(Chunk.from_range(10, 17, normal_size=3)) == [
        Chunk(offset=10, size=3),
        Chunk(offset=13, size=3),
        Chunk(offset=16, size=1),
    ]


def test_handle_delta_literal():
    chunks = list(
        handle_delta_commands([LiteralDeltaCommand(length=1), LiteralDeltaCommand(length=1)])
    )
    assert chunks == [Chunk(offset=0, size=1), Chunk(offset=1, size=1)]


def test_handle_delta_copy():
    chunks = list(
        handle_delta_commands(
            [CopyDeltaCommand(start=CHUNK_SIZE, length=CHUNK_SIZE * 2 + 100)],
            normal_chunk_size=CHUNK_SIZE
        )
    )

    assert chunks == [
        Chunk(offset=0, size=CHUNK_SIZE),
        Chunk(offset=CHUNK_SIZE, size=CHUNK_SIZE),
        Chunk(offset=CHUNK_SIZE * 2, size=100),
    ]


def test_diff_chunks():
    def chunk(hash):
        return Chunk(offset=0, size=1, hash=hash)

    old = [chunk(1), chunk(2), chunk(3)]
    new = [chunk(1), chunk(5), chunk(3), chunk(4)]

    assert diff_chunks(old, new) == {
        "new_size": 4,
        "old_size": 3,
        "preserved": 2,
        "deleted": 1,
        "added": 2
    }
