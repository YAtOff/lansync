from contextlib import contextmanager
import logging
from pathlib import Path
import time

import click
from dynaconf import settings  # type: ignore

from lansync.database import open_database
from lansync.centralized_discovery import run_discovery_loop
# from lansync.discovery import run_discovery_loop
from lansync.models import Device, StoredNode, all_models
from lansync.server import run_in_thread as run_server
from lansync.session import Session, instance as session_instance
from lansync.log import configure_logging
from lansync.node import LocalNode
from lansync import sync_action


@contextmanager
def start_session(namespace: str, root_folder: str):
    with open_database(settings.LOCAL_DB, models=all_models):
        device_id = Device.default_device_id()
        configure_logging(device_id)
        logging.info("Starting cleint with device id: %s", device_id)
        session = Session.create(namespace, root_folder, device_id)
        session_instance.configure(session)

        def on_server_start(server_port):
            run_discovery_loop(device_id, namespace, server_port, session.peer_registry)

        run_server(on_start=on_server_start)

        yield session


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.argument("namespace")
@click.argument("root_folder")
@click.argument("filename")
def send(namespace: str, root_folder: str, filename: str):
    with start_session(namespace, root_folder) as session:
        local_node = LocalNode.create(Path(filename).resolve(), session)
        stored_node = StoredNode.get_or_none(StoredNode.key == local_node.key)
        sync_action.send(local_node, stored_node, session)
        while True:
            time.sleep(1)


@cli.command()
@click.argument("namespace")
@click.argument("root_folder")
def receive(namespace: str, root_folder: str):
    with start_session(namespace, root_folder) as session:
        while True:
            remote_node = session.receive_queue.get()
            stored_node = StoredNode.get_or_none(StoredNode.key == remote_node.key)
            sync_action.receive(remote_node, stored_node, session)
            session.receive_queue.task_done()


if __name__ == "__main__":
    cli(obj={})
