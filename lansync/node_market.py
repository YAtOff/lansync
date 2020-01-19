from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Iterable, List

from lansync.market import Market


@dataclass
class NodeMarket:
    namespace: str
    key: str
    device_id: str
    market: Market
    chunk_hashes: List[str]

    @classmethod
    def for_file_provider(
        cls, namespace: str, key: str, device_id: str,
        chunk_hashes: Iterable[str], peers: Iterable[str]
    ) -> NodeMarket:
        chunk_hashes = list(sorted({h for h in chunk_hashes}))
        market = Market.for_file_provider(
            namespace=namespace, key=key, src=device_id,
            peers=peers, chunks_count=len(chunk_hashes),
        )
        market.exchange_with_db()
        return cls(
            namespace=namespace, key=key, device_id=device_id,
            market=market, chunk_hashes=chunk_hashes
        )

    @classmethod
    def for_file_consumer(
        cls, namespace: str, key: str, device_id: str,
        chunk_hashes: Iterable[str], peers: Iterable[str]
    ) -> NodeMarket:
        chunk_hashes = list(sorted({h for h in chunk_hashes}))
        market = Market.load_from_db(namespace, key)
        if market is None:
            market = Market.for_file_consumer(
                namespace=namespace, key=key, current=device_id,
                peers=peers, chunks_count=len(chunk_hashes),
            )
            market.exchange_with_db()
            logging.info("[CHUNK] Created market for [%s:%s]", namespace, key)
        return cls(
            namespace=namespace, key=key, device_id=device_id,
            market=market, chunk_hashes=chunk_hashes
        )

    def find_providers(self, chunk_hash: str) -> List[str]:
        index = self.chunk_hashes.index(chunk_hash)
        return [
            peer for peer, chunk_set in self.market.peers.items()
            if chunk_set.has(index)
        ]

    def find_consumers(self, chunk_hash: str) -> List[str]:
        index = self.chunk_hashes.index(chunk_hash)
        return [
            peer for peer, chunk_set in self.market.peers.items()
            if not chunk_set.has(index)
        ]

    def provide_chunk(self, chunk_hash: str):
        index = self.chunk_hashes.index(chunk_hash)
        self.market.peers[self.device_id] = self.market.peers[self.device_id].mark(index)
        self.market.exchange_with_db()

    def exchange(self, other_market: Market) -> Market:
        self.market.merge(other_market)
        self.market.exchange_with_db()
        return self.market
