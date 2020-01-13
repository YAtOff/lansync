from typing import Optional

from lansync.common import NodeChunk
from lansync.market import Market


class NodeChunkSet:
    def find_chunk(self, hash: str) -> Optional[NodeChunk]:
        pass

    def write_chunk(self, chunk: NodeChunk, data: bytes):
        pass

    def exchange_market(self, other_market: Market) -> Market:
        pass
