import logging

import click

from lansync.discovery import loop as run_discovery_loop
from lansync.server import run_in_thread as start_server


logging.basicConfig(level=logging.INFO)


@click.command()
@click.argument("namespace")
def main(namespace: str):
    start_server()
    run_discovery_loop(namespace)


if __name__ == "__main__":
    main()
