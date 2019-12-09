from dataclasses import asdict
from urllib.parse import urlparse, urlencode, urlunparse
from typing import Any, List, Optional

import requests

from lansync.session import Session
from lansync.node import NodeEvent, NodeOperation
from lansync.models import RemoteNode, Namespace
from lansync.serializers import NodeEventSerializer


class RemoteUrl:
    def __init__(self, session: Session):
        self.session = session

    def build_url(self, path: str, **qs) -> str:
        url_parts = list(urlparse(self.session.remote_server_url))
        url_parts[2] = path
        url_parts[4] = urlencode(qs or {})
        return urlunparse(url_parts)

    def events(self, **qs) -> str:
        return self.build_url(
            f"/namespace/{self.session.namespace}/events", **qs
        )


class RemoteClient:
    def __init__(self, session: Session):
        self.session = session
        self.remote_url = RemoteUrl(session)

    def fetch_events(self, min_sequence_number: Optional[int]) -> List[NodeEvent]:
        args = {}
        if min_sequence_number is not None:
            args["min_sequence_number"] = min_sequence_number
        response = requests.get(self.remote_url.events(**args))
        response.raise_for_status()
        data = NodeEventSerializer().load(response.json()["events"], many=True)
        return [NodeEvent(**d) for d in data]

    def push_events(self, events: List[Any]):
        data = NodeEventSerializer().dump(events, many=True)
        response = requests.post(self.remote_url.events(), json=data)
        response.raise_for_status()


class RemoteEventHandler:
    def __init__(self, session: Session):
        self.session = session

    def handle(self, event: NodeEvent):
        if event.operation == NodeOperation.CREATE:
            self.on_create(event)
        elif event.operation == NodeOperation.DELETE:
            self.on_delete(event)

    def on_create(self, event: NodeEvent):
        namespace = Namespace.by_name(self.session.namespace)  # type: ignore
        node = RemoteNode.get_or_none(
            RemoteNode.namespace == namespace,
            RemoteNode.key == event.key
        )
        attrs = asdict(event)
        if node:
            for k, v in attrs.items():
                setattr(node, k, v)
            node.save()
        else:
            RemoteNode.create(namespace=namespace, **attrs)

    def on_delete(self, event: NodeEvent):
        namespace = Namespace.by_name(self.session.namespace)  # type: ignore
        (
            RemoteNode.delete()
            .where(
                RemoteNode.namespace == namespace,
                RemoteNode.key == event.key
            )
            .execute()
        )

    def handle_new_events(self):
        namespace = Namespace.by_name(self.session.namespace)  # type: ignore
        max_sequence_number = RemoteNode.max_sequence_number(namespace)  # type: ignore
        events = RemoteClient(self.session).fetch_events(max_sequence_number)
        for event in events:
            self.handle(event)
