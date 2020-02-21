import logging
from typing import Optional

from lansync.database import atomic
from lansync.node_market import NodeMarket
from lansync.models import StoredNode
from lansync.node import RemoteNode, LocalNode, store_new_node, create_node_placeholder
from lansync.session import Session
from lansync.util.task import TaskList, Task
from lansync.util.misc import shuffled


def send(
    local_node: LocalNode, stored_node: Optional[StoredNode], session: Session
):
    logging.info("[CHUNK] New node [%s]", local_node.path)
    new_node = store_new_node(local_node, session, stored_node)

    # Create market for new node and exchange with all
    peers = session.peer_registry.peers_for_namespace(session.namespace)
    market = NodeMarket.for_file_provider(
        namespace=session.namespace,
        key=f"{local_node.key}:{local_node.checksum}",
        device_id=session.device_id,
        peers=[peer.device_id for peer in peers],
        chunk_hashes=new_node.available_chunks
    )

    tasks = TaskList()
    for peer in peers:
        client = session.client_pool.aquire(peer)
        if client is not None:
            logging.info(
                "[CHUNK] Exchange node [%s] market with [%s]", local_node.path, peer.device_id
            )
            tasks.submit(ExchangeNodeTask(client, new_node.remote_node, market, session))

    tasks.wait_all()


def receive(
    remote_node: RemoteNode, stored_node: Optional[StoredNode], session: Session
):
    logging.info("[CHUNK] Downloading node [%s]", remote_node.path)
    peer_registry = session.peer_registry
    if peer_registry.empty:
        return
    client_pool = session.client_pool
    device_id = session.device_id

    (
        stored_node,
        local_node,
        _,
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
            session.stats.emit_chunk_download(
                (session.namespace, stored_node.key, stored_node.checksum),
                self.client.peer,
                len(result)
            )
            self.chunks[0].check(result)
            with atomic():
                for chunk in self.chunks:
                    local_node.write_chunk(chunk, result)
                market.provide_chunk(self.chunk_hash)
            available_chunks.add(self.chunk_hash)

            chunk_consumers = set(market.find_consumers(self.chunk_hash))
            clients = list(client_pool.try_aquire_peers(
                (
                    peer
                    for peer in peer_registry.iter_peers(session.namespace)
                    if peer.device_id in chunk_consumers
                ),
                max_count=1
            ))
            for client in clients:
                tasks.submit(ExchangeMarketTask(client, market, session))

        def on_error(self, error):
            logging.error("[CHUNK] Error downloading chunk: %r", error)
            _, chunks = self.context
            needed_chunks.add(chunks[0].hash)

        def cleanup(self):
            client, _ = self.context
            client_pool.release(client)

    def pick_next_chunks():
        """
        TODO: try pick chunk by free clients first
        for maybe_client in clients:  # ordered by free clints per peer
            for chunk_hash in market.chunks_from_provider(maybe_client.peer.device_id):
                if chunk_hash in needed_chunks:
                    client = client_pool.aquire(maybe_client.peer)
                    if client is not None:
                        return client, chunk_index[chunk_hash]
        """
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
            self.session.stats.emit_market_exchange(
                (self.market.namespace, *self.market.key.split(":")),
                self.client.peer
            )

    def on_error(self, error):
        pass

    def cleanup(self):
        self.session.client_pool.release(self.client)


class ExchangeNodeTask(ExchangeMarketTask):
    def __init__(self, client, node, market, session):
        self.node = node
        super().__init__(client, market, session)

    def execute(self, *args, **kwargs):
        self.client.exchange_node(self.node)
        return super().execute(self, *args, **kwargs)
