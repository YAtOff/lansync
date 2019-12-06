import enum
from functools import partial
from itertools import chain, groupby
import logging
from queue import Queue
from typing import Callable, Any, List, Iterable

from lansync.session import Session
from lansync.models import RemoteNode, StoredNode, Namespace
from lansync.node import LocalNode
from lansync.sync_action import SyncActionExecutor, SyncAction
from lansync.sync_logic import handle_node
from lansync.remote import RemoteEventHandler
from lansync.util.timeout import Timeout
from lansync.util.row import Row
from lansync.util.file import iter_folder


class SyncWorkerEvent(str, enum.Enum):
    SCHEDULED_SYNC = "scheduled_sync"
    SYNC_ACTION = "sync_action"


class SyncWorker:
    def __init__(self, session: Session) -> None:
        self.session = session

        self.sync_timeout = Timeout(
            partial(self.schedule_event, SyncWorkerEvent.SCHEDULED_SYNC), interval=3
        )

        self.sync_action_producer = SyncActionProducer(session)
        self.sync_action_executor = SyncActionExecutor(session)

        self.event_queue: Any = Queue()
        self.sync_actions: List[Callable] = []

    def schedule_event(self, event: SyncWorkerEvent) -> None:
        self.event_queue.put(event)

    def run(self):
        self.schedule_event(SyncWorkerEvent.SCHEDULED_SYNC)

        while True:
            event = self.event_queue.get()
            if event == SyncWorkerEvent.SCHEDULED_SYNC:
                logging.info("[SYNC] Executing scheduled sync")
                self.do_sync()
            elif event == SyncWorkerEvent.SYNC_ACTION:
                self.do_sync_action()

    def run_once(self):
        logging.info("[SYNC] Running sync")
        self.sync_actions = self.sync_action_producer.produce()
        logging.info("[SYNC] Sync produced actions: %r", self.sync_actions)
        for action in self.sync_actions:
            logging.info("[SYNC] Executing sync action: %r", action)
            self.sync_action_executor.do_action(action)

    def do_sync(self):
        self.sync_timeout.stop()
        logging.info("[SYNC] Running sync")
        self.sync_actions = self.sync_action_producer.produce()
        logging.info("[SYNC] Sync produced actions: %r", self.sync_actions)
        self.schedule_event(SyncWorkerEvent.SYNC_ACTION)

    def do_sync_action(self):
        if self.sync_actions:
            action = self.sync_actions.pop(0)
            logging.info("[SYNC] Executing sync action: %r", action)
            self.sync_action_executor.do_action(action)
            self.schedule_event(SyncWorkerEvent.SYNC_ACTION)
        else:
            logging.info("[SYNC] Starting timer")
            self.sync_timeout.start()


class NodeRow(Row):
    value_types = [RemoteNode, LocalNode, StoredNode]


class SyncActionProducer:
    def __init__(self, session: Session):
        self.session = session

    def produce(self) -> List[SyncAction]:
        remote_nodes = handle_remote_events(self.session)
        stored_nodes = fetch_stored_nodes(self.session)
        local_nodes = scan_local_files(self.session)

        all_nodes = list(chain(remote_nodes, local_nodes, stored_nodes))
        all_nodes.sort(key=lambda n: n.key)  # type: ignore
        rows = [
            NodeRow.create(key, nodes)
            for key, nodes in groupby(all_nodes, key=lambda n: n.key)  # type: ignore
        ]
        actions = []
        for _, remote, local, stored in rows:
            actions.append(handle_node(remote, local, stored))

        return actions


def fetch_stored_nodes(session: Session) -> List[StoredNode]:
    namespace = Namespace.by_name(session.namespace)  # type: ignore
    return list(
        StoredNode.select().where(StoredNode.namespace == namespace)
    )


def handle_remote_events(session: Session) -> List[RemoteNode]:
    RemoteEventHandler(session).handle_new_events()

    namespace = Namespace.by_name(session.namespace)  # type: ignore
    return list(
        RemoteNode.select().where(RemoteNode.namespace == namespace)
    )


def scan_local_files(session: Session) -> Iterable[LocalNode]:
    return (
        LocalNode.create(p, session) for p in iter_folder(session.root_folder.path)
    )
