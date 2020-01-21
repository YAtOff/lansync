import json
import logging

from typing import NamedTuple

from lansync.discovery import Peer


class EventKey(NamedTuple):
    namespace: str
    key: str
    checksum: str


class Stats:
    def __init__(self, device_id: str):
        self.device_id = device_id
        self.logger = logging.getLogger("stats")

    def emit_chunk_download(self, key: EventKey, from_peer: Peer, size: int):
        self.emit_event(
            key,
            event="download_chunk",
            from_peer=from_peer.device_id,
            to_peer=self.device_id,
            size=size
        )

    def emit_market_exchange(self, key: EventKey, from_peer: Peer):
        self.emit_event(
            key,
            event="exchange_market",
            from_peer=from_peer.device_id,
            to_peer=self.device_id
        )

    def emit_event(self, key: EventKey, **event):
        key = EventKey(*key)
        data = {
            "namespace": key.namespace,
            "key": key.key,
            "checksum": key.checksum,
            **event
        }
        self.logger.info(json.dumps(data))
