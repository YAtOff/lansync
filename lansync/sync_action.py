import logging
from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Optional, Set

from lansync.common import NodeChunk, NodeEvent, NodeOperation
from lansync.market import Market
from lansync.models import NodeChunk as NodeChunkModel
from lansync.models import RemoteNode, RootFolder, StoredNode
from lansync.node import LocalNode
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
    event = NodeEvent(
        key=local_node.key,
        operation=NodeOperation.CREATE,
        path=local_node.path,
        timestamp=now_as_iso(),
        checksum=local_node.checksum,
        size=local_node.size,
        chunks=local_node.chunks,
    )
    RemoteClient(session).push_events([event])
    local_node.store(session, stored_node)
    RemoteEventHandler(session).handle_new_events()

    peers = session.peer_registry.peers_for_namespace(session.namespace)
    market = Market.for_file_provider(
        namespace=session.namespace,
        key=local_node.key,
        src=session.device_id,
        peers=[peer.device_id for peer in peers],
        chunks_count=len(local_node.chunks),
    )
    session.market_repo.save(market)

    tasks = TaskList()
    for peer in peers:
        client = session.client_pool.aquire(peer)
        if client is not None:
            logging.info(
                "[CHUNK] Exchange node [%s] market with [%s]", local_node.path, peer.device_id
            )
            tasks.submit(ExchangeMarketTask((client, market, session)))

    tasks.wait_all()

    return SyncActionResult()


@action
def download(
    remote_node: RemoteNode, stored_node: Optional[StoredNode], session: Session
) -> SyncActionResult:
    peer_registry = session.peer_registry
    if peer_registry.empty:
        return SyncActionResult()
    client_pool = session.client_pool
    device_id = session.device_id

    stored_node = remote_node.store(RootFolder.for_session(session), stored_node=stored_node,)
    local_node = LocalNode.create_placeholder(stored_node.local_path, stored_node.size, session)

    market = session.market_repo.load(session.namespace, remote_node.key)
    if market is None:
        market = Market.for_file_consumer(
            namespace=session.namespace,
            key=remote_node.key,
            peers=[peer.device_id for peer in peer_registry.peers_for_namespace(session.namespace)],
            chunks_count=len(remote_node.chunks),
            current=device_id,
        )
        session.market_repo.save(market)
        logging.info("[CHUNK] Created market for node [%s]", local_node.path)

    all_chunks = [NodeChunk(**c) for c in remote_node.chunks]
    needed_chunks: Set[int] = set()
    for i, chunk in enumerate(all_chunks):
        node_chunk_pair = NodeChunkModel.find(session.namespace, chunk.hash)
        if node_chunk_pair:
            node, available_chunk = node_chunk_pair
            logging.info(
                "[CHUNK] found local chunk for node [%s]: [%r]", local_node.path, available_chunk
            )
            local_node.transfer_chunk(node.local_path, available_chunk)
            NodeChunkModel.update_or_create(stored_node, available_chunk)
            market.peers[device_id] = market.peers[device_id].mark(i)
        else:
            needed_chunks.add(i)
    session.market_repo.save(market)

    tasks = TaskList()

    class DownloadChunkTask(Task):
        def execute(self, *args, **kwargs):
            client, chunk, _ = self.context
            return client.download_chunk(session.namespace, chunk.hash)

        def on_done(self, result):
            client, chunk, chunk_position = self.context
            logging.info("[CHUNK] Chunk downloaded: %s, %r", local_node.path, chunk)
            chunk.check(result)
            local_node.write_chunk(chunk, result)
            logging.info(
                "[CHUNK] Chunk written to: %r", LocalNode.create(local_node.local_path, session)
            )
            NodeChunkModel.update_or_create(stored_node, chunk)
            market.peers[device_id] = market.peers[device_id].mark(chunk_position)
            session.market_repo.save(market)

            client = client_pool.try_aquire_peer(
                peer
                for peer in peer_registry.iter_peers(session.namespace)
                if peer.device_id in market.peers
                and not market.peers[peer.device_id].has(chunk_position)
            )
            if client is not None:
                tasks.submit(ExchangeMarketTask((client, market, session)))

        def on_error(self, error):
            _, _, chunk_position = self.context
            needed_chunks.add(chunk_position)

        def cleanup(self):
            client, _, _ = self.context
            client_pool.release(client)

    def pick_next_chunk():
        for peer in peer_registry.iter_peers(session.namespace):
            peer_chunks = market.peers.get(peer.device_id)
            if peer_chunks is not None:
                for i in shuffled(needed_chunks):
                    if peer_chunks.has(i):
                        client = client_pool.aquire(peer)
                        if client is not None:
                            needed_chunks.remove(i)
                            return client, all_chunks[i], i
        return None, None, None

    while True:
        icons = [
            "✔" if market.peers[device_id].has(i) else "✖" if i in needed_chunks else "⌛"
            for i, chunk in enumerate(all_chunks)
        ]
        logging.info("[CHUNK] status: %s", " ".join(icons))

        if not needed_chunks:
            break

        client, chunk, chunk_position = pick_next_chunk()
        while client is not None:
            logging.info("[CHUNK] Downloading chunk: %s, %r", local_node.path, chunk)
            tasks.submit(DownloadChunkTask((client, chunk, chunk_position)))
            client, chunk, chunk_position = pick_next_chunk()

        if not tasks:
            logging.info(
                "[CHUNK] No chunks for [%s] found no market; doing exchange", local_node.path
            )
            for peer in peer_registry.iter_peers(session.namespace):
                client = client_pool.aquire(peer)
                if client is not None:
                    tasks.submit(ExchangeMarketTask((client, market, session)))

        tasks.wait_any()

    stored_node.sync_with_local(local_node)

    return SyncActionResult()


@action
def delete_local(
    local_node: LocalNode, stored_node: StoredNode, session: Session
) -> SyncActionResult:
    local_node.local_path.unlink()
    stored_node.delete().execute()
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
    stored_node.delete().execute()
    RemoteEventHandler(session).handle_new_events()
    return SyncActionResult()


@action
def save_stored(
    remote_node: RemoteNode, local_node: LocalNode, session: Session
) -> SyncActionResult:
    local_node.store(session, None)
    return SyncActionResult()


@action
def delete_stored(stored_node: StoredNode, session: Session) -> SyncActionResult:
    stored_node.delete().execute()
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
    def execute(self, *args, **kwargs):
        client, market, _ = self.context
        return client.exchange_market(market)

    def on_done(self, result):
        client, market, session = self.context
        logging.info("[CHUNK] Market exchanged")
        if result is not None:
            market.merge(result)
            session.market_repo.save(market)

    def on_error(self, error):
        pass

    def cleanup(self):
        client, _, session = self.context
        session.client_pool.release(client)
