import logging

import click

from lansync.discovery import loop as run_discovery_loop


logging.basicConfig(level=logging.INFO)


@click.command()
@click.argument("namespace")
def main(namespace: str):
    run_discovery_loop(namespace)


if __name__ == "__main__":
    main()
