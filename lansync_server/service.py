from typing import List, Dict

from lansync_server.models import Namespace, NodeEvent, Sequence
from lansync_server.serializers import NodeEventSerializer


def store_events(namespace_name: str, data) -> int:
    namespace, _ = Namespace.get_or_create(name=namespace_name)
    events = NodeEventSerializer().load(data, many=True)
    sequence_number = 0
    for event in events:
        sequence_number = Sequence.increment(namespace_name)
        NodeEvent.create(
            namespace=namespace,
            sequence_number=sequence_number,
            **event
        )
    return sequence_number


def load_events(namespace_name: str, min_sequence_number: int) -> List[Dict]:
    namespace, _ = Namespace.get_or_create(name=namespace_name)
    nodes = list(
        NodeEvent.select()
        .where(
            NodeEvent.namespace == namespace,
            NodeEvent.sequence_number >= min_sequence_number
        )
    )
    return NodeEventSerializer().dump(nodes, many=True)
