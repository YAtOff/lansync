from __future__ import annotations

import math
import random
from dataclasses import dataclass
from itertools import zip_longest
from typing import Any, Dict, Iterable, Optional, Tuple

from lansync.database import atomic
from lansync.avro_serializer import SerializerMixin
from lansync import models


@dataclass
class ChunkSet:
    chunks_count: int
    chunks: bytes

    @classmethod
    def empty(cls, chunks_count: int) -> ChunkSet:
        return cls(chunks_count, bytes(math.ceil(chunks_count / 8)))

    @classmethod
    def full(cls, chunks_count: int) -> ChunkSet:
        return cls(chunks_count, b"\xFF" * math.ceil(chunks_count / 8))

    def has(self, position: int) -> bool:
        byte = self.chunks[position // 8]
        return (byte & (1 << (position % 8))) != 0

    def has_all(self) -> bool:
        return all(self.has(p) for p in range(self.chunks_count))

    def mark(self, position: int) -> ChunkSet:
        chunks = bytearray(self.chunks)
        chunks[position // 8] |= 1 << (position % 8)
        return ChunkSet(self.chunks_count, bytes(chunks))

    def merge(self, other: ChunkSet) -> ChunkSet:
        return ChunkSet(
            max(self.chunks_count, other.chunks_count),
            bytes(x | y for x, y in zip_longest(self.chunks, other.chunks, fillvalue=0x0))
        )

    def diff(self, other: ChunkSet) -> ChunkSet:
        return ChunkSet(
            max(self.chunks_count, other.chunks_count),
            bytes(x & (~y) for x, y in zip_longest(self.chunks, other.chunks, fillvalue=0x0))
        )

    def pick_random(self) -> Optional[int]:
        marked = []
        offset = 0
        for byte in self.chunks:
            for i in range(8):
                if offset + i >= self.chunks_count:
                    break
                if byte & 0x1:
                    marked.append(offset + i)
                byte = byte >> 1
            offset += 8
        return random.choice(marked) if marked else None


@dataclass
class Market(SerializerMixin):
    schema_name = "market"

    namespace: str
    key: str
    peers: Dict[str, ChunkSet]

    @classmethod
    def from_record(cls, record: Dict[str, Any]) -> Market:
        return cls(
            namespace=record["namespace"],
            key=record["key"],
            peers={
                p["device_id"]: ChunkSet(p["chunks_count"], p["chunks"])
                for p in record["peers"]
            },
        )

    @classmethod
    def for_file_provider(
        cls, namespace: str, key: str, peers: Iterable[str],
        chunks_count: int, src: str
    ) -> Market:
        market = Market(
            namespace=namespace,
            key=key,
            peers={peer: ChunkSet.empty(chunks_count) for peer in peers},
        )
        market.peers[src] = ChunkSet.full(chunks_count)
        return market

    @classmethod
    def for_file_consumer(
        cls, namespace: str, key: str, peers: Iterable[str],
        chunks_count: int, current: str
    ) -> Market:
        market = Market(
            namespace=namespace,
            key=key,
            peers={peer: ChunkSet.empty(chunks_count) for peer in peers},
        )
        market.peers[current] = ChunkSet.empty(chunks_count)
        return market

    @classmethod
    def load_from_db(cls, namespace: str, key: str) -> Optional[Market]:
        db_instance = models.Market.find(namespace, key)
        return cls.load(db_instance.data) if db_instance is not None else None

    def exchange_with_db(self):
        with atomic():
            db_instance = models.Market.find(self.namespace, self.key)
            if db_instance is None:
                models.Market.create(
                    namespace=models.Namespace.by_name(self.namespace),
                    key=self.key,
                    data=self.dump()
                )
            else:
                other = Market.load(db_instance.data)
                self.merge(other)
                db_instance.data = self.dump()
                db_instance.save()

    def as_record(self) -> Dict[str, Any]:
        return {
            "namespace": self.namespace,
            "key": self.key,
            "peers": [
                {
                    "device_id": device_id,
                    "chunks_count": chunk_set.chunks_count,
                    "chunks": chunk_set.chunks
                }
                for device_id, chunk_set in self.peers.items()
            ],
        }

    def merge(self, other: Market) -> None:
        for device_id, chunk_set in other.peers.items():
            if device_id in self.peers:
                self.peers[device_id] = self.peers[device_id].merge(chunk_set)
            else:
                self.peers[device_id] = other.peers[device_id]


MarketKey = Tuple[str, str]
