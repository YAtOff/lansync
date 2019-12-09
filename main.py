import logging

import click
from dynaconf import settings  # type: ignore

# from lansync.discovery import loop as run_discovery_loop
# from lansync.server import run_in_thread as start_server
from lansync.database import open_database
from lansync.models import all_models
from lansync.session import Session
from lansync.sync import SyncWorker


logging.basicConfig(level=logging.INFO)


@click.command()
@click.argument("namespace")
@click.option("--once/--no-once", default=False)
def main(namespace: str, once: bool):
    # start_server()
    # run_discovery_loop(namespace)
    with open_database(settings.LOCAL_DB, models=all_models):
        worker = SyncWorker(Session.create(namespace))
        if once:
            worker.run_once()
        else:
            worker.run()


if __name__ == "__main__":
    main()
