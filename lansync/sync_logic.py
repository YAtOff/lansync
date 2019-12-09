from lansync.models import StoredNode, RemoteNode
from lansync.node import LocalNode
from lansync.sync_action import (
    SyncAction,
    download,
    upload,
    delete_local,
    delete_remote,
    save_stored,
    delete_stored,
    conflict,
    nop,
)


def handle_node(
    remote: RemoteNode, local: LocalNode, stored: StoredNode
) -> SyncAction:
    if not remote and not local and not stored:
        return nop()
    elif not remote and not local and stored:
        return delete_stored(stored)
    elif not remote and local and not stored:
        return upload(local, None)
    elif not remote and local and stored:
        return delete_local(local, stored)
    elif remote and not local and not stored:
        return download(remote, None)
    elif remote and not local and stored:
        return delete_remote(remote, stored)
    elif remote and local and not stored:
        if remote.checksum == local.checksum:
            return save_stored(remote, local)
        else:
            return conflict(remote, local, stored)
    elif remote and local and stored:
        local_updated = local.updated(stored)
        remote_updated = remote.updated(stored)
        if local_updated and remote_updated:
            if remote.checksum == local.checksum:
                return save_stored(remote, local)
            else:
                return conflict(remote, local, stored)
        elif local_updated:
            return upload(local, stored)
        elif remote_updated:
            return download(remote, stored)
        else:
            return nop()
    return nop()
