import logging

import click
from dynaconf import settings  # type: ignore

from lansync.database import open_database
from lansync.discovery import run_discovery_loop
from lansync.models import Device, all_models
from lansync.server import run_in_thread as run_server
from lansync.session import Session, instance as session_instance
from lansync.sync import SyncWorker
from lansync.log import configure_logging


@click.command()
@click.argument("namespace")
@click.argument("root_folder")
@click.option("--once/--no-once", default=False)
def main(namespace: str, root_folder: str, once: bool):
    with open_database(settings.LOCAL_DB, models=all_models):
        device_id = Device.default_device_id()
        configure_logging(device_id)
        logging.info("Starting cleint with device id: %s", device_id)
        session = Session.create(namespace, root_folder, device_id)
        session_instance.configure(session)

        def on_server_start(server_port):
            run_discovery_loop(device_id, namespace, server_port, session.peer_registry)

        run_server(on_start=on_server_start)

        worker = SyncWorker(session)
        session.sync_worker = worker
        if once:
            worker.run_once()
        else:
            worker.run()


if __name__ == "__main__":
    main()
