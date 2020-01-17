from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Set

from lansync.common import NodeChunk
from lansync.market import Market
from lansync.models import RemoteNode, StoredNode
from lansync.node import LocalNode
from lansync.session import Session


@dataclass
class NodeMarket:
    session: Session
    namespace: str
    marker: Market
    remote_node: RemoteNode
    local_node: LocalNode
    stored_node: StoredNode
    missing_chunks: Set[NodeChunk]

    @classmethod
    def create(
        cls,
        session: Session,
        namespace: str,
        marker: Market,
        remote_node: RemoteNode,
        local_node: LocalNode,
        stored_node: StoredNode
    ) -> NodeMarket:
        pass

    def book_chunk(self) -> Optional[NodeChunk]:
        pass

    def release_chunk(self, chunk: NodeChunk):
        pass

    def confirm_chunk(self, chunk: NodeChunk):
        pass

    def exchange(self, other_market: Market) -> Market:
        pass
