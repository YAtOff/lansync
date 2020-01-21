#!/usr/bin/env python

import os
import re
from pathlib import Path
import shutil
from random import randint

import click
from dynaconf import settings  # type: ignore
from faker import Faker, providers

from rschunks.delta import parse_delta_from_file


fake = Faker()
fake.add_provider(providers.misc)
fake.add_provider(providers.file)

storage_folder = Path(settings.STORAGE_FOLDER)
metadata_folder = Path(settings.METADATA_FOLDER)


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.pass_context
def reset(ctx):
    for dir in (storage_folder, metadata_folder):
        shutil.rmtree(dir)
        dir.mkdir()


@cli.command()
@click.pass_context
@click.argument("filename")
@click.option("--position", default=-1, type=int)
@click.option("--size", default=-1, type=int)
def mutate_file(ctx, filename: str, position: int, size: int):
    with open(filename, "r+b") as f:
        f.seek(0, os.SEEK_END)
        file_size = f.tell()

        position = position if position != -1 \
            else randint(0, file_size)
        assert position < file_size

        size = size if size != -1 else randint(1, file_size - position)
        assert size < file_size - position

        f.seek(position, os.SEEK_SET)
        f.write(fake.binary(size))


def make_chunk(size: int) -> bytes:
    return randint(10, 15).to_bytes(1, 'little') * size


def parse_size(size: str) -> int:
    match = re.match(r'^(\d+)([KMG]?)$', size)
    if not match:
        raise Exception("Invalid size", size)
    number, multiplier_code = match.groups()
    multiplier = {
        "K": 1024,
        "M": 1024 * 1024,
        "G": 1024 * 1024 * 1024,
    }[multiplier_code]
    return int(number) * multiplier


@cli.command()
@click.pass_context
@click.argument("size")
def make_file(ctx, size: str):
    size_in_bytes = parse_size(size)
    filename = fake.file_name()
    with open(storage_folder / filename, "wb") as f:
        bytes_left = size_in_bytes
        while bytes_left > 0:
            chunk_size = min(settings.CHUNK_SIZE, bytes_left)
            f.write(make_chunk(chunk_size))
            bytes_left -= chunk_size

    print(filename)


@cli.command()
@click.pass_context
@click.argument("filename")
def parse_delta(ctx, filename):
    for command in parse_delta_from_file(filename):
        print(command)


if __name__ == "__main__":
    cli(obj={})
