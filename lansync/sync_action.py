from dataclasses import dataclass
from functools import partial, wraps
from typing import Callable, Optional

from lansync.models import StoredNode, RemoteNode, RootFolder, Namespace
from lansync.node import LocalNode, NodeEvent, NodeOperation
from lansync.remote import RemoteClient, RemoteEventHandler
from lansync.session import Session
from lansync.file_transfer import download_from_peer
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
    local_node: LocalNode, stored_node: Optional[StoredNode],
    session: Session
) -> SyncActionResult:
    event = NodeEvent(
        key=local_node.key,
        operation=NodeOperation.CREATE,
        path=local_node.path,
        timestamp=now_as_iso(),
        checksum=local_node.checksum,
        parts=local_node.parts
    )

    RemoteClient(session).push_events([event])

    if stored_node is not None:
        stored_node.checksum = local_node.checksum
        stored_node.parts = local_node.parts  # type: ignore
        stored_node.local_modified_time = local_node.modified_time
        stored_node.local_created_time = local_node.created_time
        stored_node.save()
    else:
        StoredNode.create(
            namespace=Namespace.for_session(session),
            root_folder=RootFolder.for_session(session),
            key=local_node.key,
            path=local_node.path,
            checksum=local_node.checksum,
            parts=local_node.parts,
            local_modified_time=local_node.modified_time,
            local_created_time=local_node.created_time
        )

    RemoteEventHandler(session).handle_new_events()
    return SyncActionResult()


@action
def download(
    remote_node: RemoteNode, stored_node: Optional[StoredNode],
    session: Session
) -> SyncActionResult:
    local_path = session.root_folder.path / remote_node.path
    if not download_from_peer(remote_node, local_path, session):
        return SyncActionResult()
    local_node = LocalNode.create(local_path, session)
    if stored_node is not None:
        stored_node.checksum = local_node.checksum
        stored_node.parts = local_node.parts  # type: ignore
        stored_node.local_modified_time = local_node.modified_time
        stored_node.local_created_time = local_node.created_time
        stored_node.save()
    else:
        StoredNode.create(
            namespace=remote_node.namespace,
            root_folder=RootFolder.for_session(session),
            key=local_node.key,
            path=local_node.path,
            checksum=local_node.checksum,
            parts=local_node.parts,
            local_modified_time=local_node.modified_time,
            local_created_time=local_node.created_time
        )

    return SyncActionResult()


@action
def delete_local(
    local_node: LocalNode, stored_node: StoredNode,
    session: Session
) -> SyncActionResult:
    local_node.local_path.unlink()
    stored_node.delete().execute()
    return SyncActionResult()


@action
def delete_remote(
    remote_node: RemoteNode, stored_node: StoredNode,
    session: Session,
) -> SyncActionResult:
    event = NodeEvent(
        key=remote_node.key,
        operation=NodeOperation.DELETE,
        path=remote_node.path,
        timestamp=now_as_iso()
    )
    RemoteClient(session).push_events([event])
    stored_node.delete().execute()
    RemoteEventHandler(session).handle_new_events()
    return SyncActionResult()


@action
def save_stored(
    remote_node: RemoteNode, local_node: LocalNode,
    session: Session
) -> SyncActionResult:
    StoredNode.create(
        namespace=remote_node.namespace,
        root_folder=RootFolder.for_session(session),
        key=remote_node.key,
        path=remote_node.path,
        checksum=remote_node.checksum,
        parts=remote_node.parts,
        local_modified_time=local_node.modified_time,
        local_created_time=local_node.created_time
    )
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
    session: Session
) -> SyncActionResult:
    return SyncActionResult()


@action
def nop(session: Session) -> SyncActionResult:
    return SyncActionResult()
