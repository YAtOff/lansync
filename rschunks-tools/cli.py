#!/usr/bin/env python

import json
import os
from pathlib import Path
from pprint import pprint
import shutil

import click
from dynaconf import settings  # type: ignore

import librsync

from rschunks.delta import parse_delta_from_file
from rschunks.chunk import (
    read_chunks_from_file,
    update_chunks,
    serialize_file_chunks,
    deserialize_file_chunks,
    diff_chunks
)


storage_folder = Path(settings.STORAGE_FOLDER)
metadata_folder = Path(settings.METADATA_FOLDER)


@click.group()
@click.pass_context
def cli(ctx):
    pass


@cli.command()
@click.pass_context
@click.argument("filename")
def init(ctx, filename: str):
    assert storage_folder in Path(filename).parents
    file_metadata_dir = metadata_folder / Path(filename).name

    if file_metadata_dir.exists():
        shutil.rmtree(file_metadata_dir)
    file_metadata_dir.mkdir(parents=True)

    signature = os.fspath(file_metadata_dir / "signature")
    librsync.signature_from_paths(filename, signature, block_len=settings.CHUNK_SIZE)
    with open(file_metadata_dir / "chunks.json", "wt") as f:
        json.dump(serialize_file_chunks(read_chunks_from_file(filename)), f, indent=2)


@cli.command()
@click.pass_context
@click.argument("filename")
def update(ctx, filename: str):
    assert storage_folder in Path(filename).parents
    file_metadata_dir = metadata_folder / Path(filename).name
    signature = os.fspath(file_metadata_dir / "signature")
    delta = os.fspath(file_metadata_dir / "delta")
    librsync.delta_from_paths(signature, filename, delta)
    delta_commands = list(parse_delta_from_file(delta))
    new_chunks = list(update_chunks(delta_commands, filename))

    chunks_file = file_metadata_dir / f"chunks.json"
    prev_chunks_file = file_metadata_dir / f"chunks-prev.json"
    if prev_chunks_file.exists():
        prev_chunks_file.unlink()
    shutil.move(chunks_file, prev_chunks_file)
    with open(chunks_file, "wt") as f:
        json.dump(serialize_file_chunks(new_chunks), f, indent=2)

    with open(prev_chunks_file, "rt") as f:
        old_chunks = deserialize_file_chunks(json.load(f))

    pprint(diff_chunks(old_chunks, new_chunks))


if __name__ == "__main__":
    cli(obj={})
