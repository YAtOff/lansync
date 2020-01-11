from contextlib import contextmanager
from pathlib import Path
from threading import RLock

import peewee  # type: ignore


database = peewee.DatabaseProxy()


@contextmanager
def open_database(path, models=None):
    database_exists = path != ":memory:" and Path(path).exists()
    database.initialize(
        peewee.SqliteDatabase(path, pragmas={"foreign_keys": 1, "journal_mode": "wal"})
    )
    try:
        database.connect()
        if not database_exists and models is not None:
            database.create_tables(models)
        yield database
    finally:
        database.close()


transaction_lock = RLock()


@contextmanager
def atomic():
    with transaction_lock:
        with database.atomic():
            yield
