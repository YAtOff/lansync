import logging
from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Optional

from lansync.common import NodeEvent, NodeOperation
from lansync.database import atomic
from lansync.node_market import NodeMarket
from lansync.models import RemoteNode, StoredNode
from lansync.node import LocalNode, store_new_node, create_node_placeholder
from lansync.remote import RemoteClient, RemoteEventHandler
from lansync.session import Session
from lansync.util.task import TaskList, Task
from lansync.util.misc import shuffled
from lansync.util.timeutil import now_as_iso


@dataclass
class SyncActionResult:
    pass


class SyncAction:
    def __init__(self, action: Callable):
        self.action = action

    def __call__(self, *args, **kwargs):
        return self.action(*args, **kwargs)

    def __repr__(self) -> str:
        return f"{self.action.func.__name__}({self.action.args, self.action.keywords})"  # type: ignore


class SyncActionExecutor:
    def __init__(self, session: Session):
        self.session = session

    def do_action(self, action: SyncAction) -> SyncActionResult:
        return action(self.session)


def action(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable[[Session], SyncActionResult]:
        return SyncAction(partial(func, *args, **kwargs))

    return wrapper


@action
def upload(
    local_node: LocalNode, stored_node: Optional[StoredNode], session: Session
) -> SyncActionResult:
    logging.info("[CHUNK] New node [%s]", local_node.path)
    stored_node, _, all_chunks, _, _, available_chunks = store_new_node(local_node, session, stored_node)

    # Publish event for new node
    event = NodeEvent(
        key=local_node.key,
        operation=NodeOperation.CREATE,
        path=local_node.path,
        timestamp=now_as_iso(),
        checksum=local_node.checksum,
        size=local_node.size,
        chunks=all_chunks,
        signature=stored_node.signature
    )
    RemoteClient(session).push_events([event])
    RemoteEventHandler(session).handle_new_events()

    # Create market for new node and exchange with all
    peers = session.peer_registry.peers_for_namespace(session.namespace)
    market = NodeMarket.for_file_provider(
        namespace=session.namespace,
        key=f"{local_node.key}:{local_node.checksum}",
        device_id=session.device_id,
        peers=[peer.device_id for peer in peers],
        chunk_hashes=available_chunks
    )

    tasks = TaskList()
    for peer in peers:
        client = session.client_pool.aquire(peer)
        if client is not None:
            logging.info(
                "[CHUNK] Exchange node [%s] market with [%s]", local_node.path, peer.device_id
            )
            tasks.submit(ExchangeMarketTask(client, market, session))

    tasks.wait_all()

    return SyncActionResult()


@action
def download(
    remote_node: RemoteNode, stored_node: Optional[StoredNode], session: Session
) -> SyncActionResult:
    logging.info("[CHUNK] Downloading node [%s]", remote_node.path)
    peer_registry = session.peer_registry
    if peer_registry.empty:
        return SyncActionResult()
    client_pool = session.client_pool
    device_id = session.device_id

    (
        stored_node,
        local_node,
        all_chunks,
        chunk_index,
        needed_chunks,
        available_chunks
    ) = create_node_placeholder(remote_node, session)

    market = NodeMarket.for_file_consumer(
        namespace=session.namespace,
        key=f"{remote_node.key}:{remote_node.checksum}",
        device_id=device_id,
        peers=[peer.device_id for peer in peer_registry.peers_for_namespace(session.namespace)],
        chunk_hashes=needed_chunks | available_chunks
    )
    for chunk_hash in available_chunks:
        market.provide_chunk(chunk_hash)

    tasks = TaskList()

    class DownloadChunkTask(Task):
        def __init__(self, client, chunks):
            self.client = client
            self.chunks = chunks
            self.chunk_hash = chunks[0].hash
            super().__init__((client, chunks))

        def execute(self, *args, **kwargs):
            return self.client.download_chunk(session.namespace, self.chunk_hash)

        def on_done(self, result):
            logging.info(
                "[CHUNK] Chunk downloaded [%s:%r] form %s",
                local_node.path, self.chunk_hash, self.client.peer.device_id
            )
            self.chunks[0].check(result)
            with atomic():
                for chunk in self.chunks:
                    local_node.write_chunk(chunk, result)
                market.provide_chunk(self.chunk_hash)
            available_chunks.add(self.chunk_hash)

            chunk_consumers = set(market.find_consumers(self.chunk_hash))
            client = client_pool.try_aquire_peer(
                peer
                for peer in peer_registry.iter_peers(session.namespace)
                if peer.device_id in chunk_consumers
            )
            if client is not None:
                tasks.submit(ExchangeMarketTask(client, market, session))

        def on_error(self, error):
            logging.error("[CHUNK] Error downloading chunk: %r", error)
            _, chunks = self.context
            needed_chunks.add(chunks[0].hash)

        def cleanup(self):
            client, _ = self.context
            client_pool.release(client)

    def pick_next_chunks():
        live_peers = {
            peer.device_id: peer
            for peer in peer_registry.live_peers(session.namespace)
        }
        peer_chunk_pairs = (
            (peer, chunk_hash)
            for chunk_hash in shuffled(needed_chunks)
            for device_id in market.find_providers(chunk_hash)
            for peer in (live_peers.get(device_id),)
            if peer is not None
        )
        for peer, chunk_hash in peer_chunk_pairs:
            client = client_pool.aquire(peer)
            if client is not None:
                needed_chunks.remove(chunk_hash)
                return client, chunk_index[chunk_hash]
        return None, None

    while True:
        icons = [
            "✔" if chunk_hash in available_chunks
            else "✖" if chunk_hash in needed_chunks
            else "⌛"
            for chunk_hash in chunk_index.keys()
        ]
        logging.info("[CHUNK] status: %s", " ".join(icons))

        if len(available_chunks) == len(chunk_index):
            break

        client, chunks = pick_next_chunks()
        while client is not None:
            logging.info(
                "[CHUNK] Downloading chunk [%s:%r] from %s",
                local_node.path, chunks[0].hash, client.peer.device_id
            )
            tasks.submit(DownloadChunkTask(client, chunks))
            client, chunks = pick_next_chunks()

        if tasks.empty:
            logging.info("[CHUNK] No chunks for [%s] found no market", local_node.path)
            for peer in peer_registry.iter_peers(session.namespace):
                client = client_pool.aquire(peer)
                if client is not None:
                    logging.info("[CHUNK] Doing exchange with: %s", peer.device_id)
                    tasks.submit(ExchangeMarketTask(client, market, session))

        tasks.wait_any()

    stored_node.sync_with_local(local_node)

    return SyncActionResult()


@action
def delete_local(
    local_node: LocalNode, stored_node: StoredNode, session: Session
) -> SyncActionResult:
    local_node.local_path.unlink()
    stored_node.delete_instance()
    return SyncActionResult()


@action
def delete_remote(
    remote_node: RemoteNode, stored_node: StoredNode, session: Session,
) -> SyncActionResult:
    event = NodeEvent(
        key=remote_node.key,
        operation=NodeOperation.DELETE,
        path=remote_node.path,
        timestamp=now_as_iso(),
    )
    RemoteClient(session).push_events([event])
    stored_node.delete_instance()
    RemoteEventHandler(session).handle_new_events()
    return SyncActionResult()


@action
def save_stored(
    remote_node: RemoteNode, local_node: LocalNode, session: Session
) -> SyncActionResult:
    store_new_node(local_node, session, None)
    return SyncActionResult()


@action
def delete_stored(stored_node: StoredNode, session: Session) -> SyncActionResult:
    stored_node.delete_instance()
    return SyncActionResult()


@action
def conflict(
    remote_node: RemoteNode,
    local_node: LocalNode,
    stored_node: Optional[StoredNode],
    session: Session,
) -> SyncActionResult:
    return SyncActionResult()


@action
def nop(session: Session) -> SyncActionResult:
    return SyncActionResult()


class ExchangeMarketTask(Task):
    def __init__(self, client, market, session):
        self.client = client
        self.market = market
        self.session = session
        super().__init__((client, market, session))

    def execute(self, *args, **kwargs):
        return self.client.exchange_market(self.market.market)

    def on_done(self, result):
        logging.info("[CHUNK] Market exchanged with %s", self.client.peer.device_id)
        if result is not None:
            self.market.exchange(result)

    def on_error(self, error):
        pass

    def cleanup(self):
        self.session.client_pool.release(self.client)
